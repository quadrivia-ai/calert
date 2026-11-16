package google_chat

// Tests for TECH-581. calert must name the HTTP status and the response body
// Google Chat returned on a rejected dispatch, must stay quiet on a delivered
// one, and must never write the webhook key or token into a log line.

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mr-karan/calert/internal/metrics"
	alertmgrtmpl "github.com/prometheus/alertmanager/template"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// credentialQuery is the shape of a real Google Chat incoming webhook: the key
// and the token ride in the query string, which is why nothing may log the URL.
const credentialQuery = "?key=KEY-MUST-NOT-APPEAR&token=TOKEN-MUST-NOT-APPEAR"

// chatWithLogBuffer returns a provider pointed at endpoint, plus the buffer
// holding every log line it writes, at debug level so nothing is filtered out.
func chatWithLogBuffer(t *testing.T, endpoint string, retryMax int) (*GoogleChatManager, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	chat, err := NewGoogleChat(GoogleChatOpts{
		Log:          slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Metrics:      metrics.New("calert"),
		Endpoint:     endpoint,
		Room:         "alerts-critical",
		Template:     "../../../static/message.tmpl",
		RetryMax:     retryMax,
		RetryWaitMin: time.Millisecond,
		RetryWaitMax: 2 * time.Millisecond,
	})
	require.NoError(t, err)
	return chat, logs
}

// assertNoCredential is DoD 2: the full log output of a dispatch carries no
// webhook credential, in any form.
func assertNoCredential(t *testing.T, out string) {
	t.Helper()
	for _, banned := range []string{"key=", "token=", "KEY-MUST-NOT-APPEAR", "TOKEN-MUST-NOT-APPEAR"} {
		assert.NotContainsf(t, out, banned, "log output must not contain %q", banned)
	}
}

// TestRejectedDispatchNamesStatusAndBody is DoD 1 for the failure: a 502, which
// is the status the 2026-10-28 episode returned and the one retryablehttp
// retries, so it is the case that used to go dark entirely.
func TestRejectedDispatchNamesStatusAndBody(t *testing.T) {
	const googleError = `{"error":{"code":502,"message":"Bad Gateway","status":"UNAVAILABLE"}}`

	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(googleError))
	}))
	defer srv.Close()

	chat, logs := chatWithLogBuffer(t, srv.URL+credentialQuery, 1)
	err := chat.pushAlerts(context.Background(), []alertmgrtmpl.Alert{
		{Status: "firing", Labels: alertmgrtmpl.KV{"alertname": "CriticalHTTP5xxErrorRate"}},
	})
	require.Error(t, err)
	require.Equal(t, int32(2), attempts.Load(), "the 502 must still be retried")

	out := logs.String()
	assert.Contains(t, out, `"status":502`)
	assert.Contains(t, out, `"response_body":`)
	assert.Contains(t, out, "UNAVAILABLE", "Google's own reason must reach the log")
	assert.Contains(t, out, `"room":"alerts-critical"`)
	assert.Contains(t, out, `"provider":"google_chat"`)
	assert.Contains(t, out, `"attempt":1`)
	assert.Contains(t, out, `"attempt":2`)
	assert.Contains(t, out, `"will_retry":true`)
	assert.Contains(t, out, `"will_retry":false`, "the attempt that gives up must be named too")
	assertNoCredential(t, out)

	// The counter labels the status, on the metric name that is already scraped.
	var promOut bytes.Buffer
	chat.metrics.FlushMetrics(&promOut)
	assert.Contains(t, promOut.String(),
		`calert_alerts_dispatched_errors_total{provider="google_chat", room="alerts-critical", reason="sending", status="502"}`)
}

// TestDeliveredDispatchNamesNoStatus is the control for DoD 1 and DoD 2: a
// successful dispatch logs no status and no error, and is clean too.
func TestDeliveredDispatchNamesNoStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	chat, logs := chatWithLogBuffer(t, srv.URL+credentialQuery, 1)
	require.NoError(t, chat.pushAlerts(context.Background(), []alertmgrtmpl.Alert{
		{Status: "firing", Labels: alertmgrtmpl.KV{"alertname": "CriticalHTTP5xxErrorRate"}},
	}))

	out := logs.String()
	require.NotEmpty(t, out, "the control must actually have logged something")
	assert.NotContains(t, out, `"status"`)
	assert.NotContains(t, out, `"response_body"`)
	assert.NotContains(t, out, `"error"`)
	assert.NotContains(t, out, "did not accept")
	assertNoCredential(t, out)
}

// TestTransportFailureLabelsStatusNone separates "Google said no" from "nothing
// came back at all", and proves the transport error's text — which carries the
// full URL — never reaches the log.
func TestTransportFailureLabelsStatusNone(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := srv.URL + credentialQuery
	srv.Close() // nothing is listening, so every attempt fails to connect.

	chat, logs := chatWithLogBuffer(t, endpoint, 0)
	require.Error(t, chat.pushAlerts(context.Background(), []alertmgrtmpl.Alert{
		{Status: "firing", Labels: alertmgrtmpl.KV{"alertname": "CriticalHTTP5xxErrorRate"}},
	}))

	out := logs.String()
	assert.Contains(t, out, `"status":0`)
	assertNoCredential(t, out)
	assert.NotContains(t, out, strings.TrimPrefix(endpoint, "http://"))

	var promOut bytes.Buffer
	chat.metrics.FlushMetrics(&promOut)
	assert.Contains(t, promOut.String(),
		`calert_alerts_dispatched_errors_total{provider="google_chat", room="alerts-critical", reason="sending", status="none"}`)
}

// TestResponseBodyIsRestoredForTheRetryLogic proves reading the body to log it
// does not change what the retry logic reads, and that Close still reaches the
// real body rather than a no-op.
func TestResponseBodyIsRestoredForTheRetryLogic(t *testing.T) {
	body := strings.Repeat("x", chatResponseBodyLimit*2)
	closed := false
	resp := &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &closeTrackingBody{Reader: strings.NewReader(body), closed: &closed},
	}

	logged := readChatResponseBody(resp)
	assert.Len(t, logged, chatResponseBodyLimit, "only the capped prefix is logged")

	rest, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, body, string(rest), "the retry logic must still see the whole body")

	require.NoError(t, resp.Body.Close())
	assert.True(t, closed, "Close must reach the real body, not an io.NopCloser")
}

type closeTrackingBody struct {
	io.Reader
	closed *bool
}

func (b *closeTrackingBody) Close() error {
	*b.closed = true
	return nil
}

// TestCheckRetryToleratesResponseWithoutBody guards the path the existing
// policy tests use, where the response is synthesised with a nil Body.
func TestCheckRetryToleratesResponseWithoutBody(t *testing.T) {
	chat, _ := chatWithLogBuffer(t, "https://chat.example.test/webhook"+credentialQuery, 1)
	shouldRetry, err := chat.client.CheckRetry(context.Background(), &http.Response{StatusCode: http.StatusBadGateway}, nil)
	require.NoError(t, err)
	assert.True(t, shouldRetry)
}
