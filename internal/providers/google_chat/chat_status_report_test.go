package google_chat

// Tests for TECH-581. calert must name the HTTP status and the response body
// Google Chat returned on a rejected dispatch, must stay quiet on a delivered
// one, and must never write the webhook key or token into a log line.

import (
	"bytes"
	"context"
	"fmt"
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

// bodySentinel is the stand-in for text Google put in a response body. It must
// appear in the log for the statuses whose body is Google talking about itself,
// and must never appear for the statuses where Google may be quoting us.
const bodySentinel = "GOOGLE-RESPONSE-BODY-SENTINEL"

// dispatchOneAttempt sends a single alert to a server that answers with status
// and bodySentinel, and returns the whole log output. RetryMax is 0, so there
// is exactly 1 attempt and exactly 1 report line.
func dispatchOneAttempt(t *testing.T, status int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":{"message":"` + bodySentinel + `"}}`))
	}))
	defer srv.Close()

	chat, logs := chatWithLogBuffer(t, srv.URL+credentialQuery, 0)
	err := chat.pushAlerts(context.Background(), []alertmgrtmpl.Alert{
		{Status: "firing", Labels: alertmgrtmpl.KV{"alertname": "CriticalHTTP5xxErrorRate"}},
	})
	require.Error(t, err, "a non-2xx must fail the dispatch")
	return logs.String()
}

// TestFourxxResponseBodyIsNotLogged is the half of the boundary that upstream's
// TestSendMessageSanitizesTransportAndResponseErrors guarantees, kept as a
// first-class test so the line is tested and not merely asserted in prose.
//
// Google rejects a 4xx request it has already read, so an INVALID_ARGUMENT body
// can name a field we sent. The Chat card is built from alert labels, and
// nothing in this estate screens those for PHI. The status must still be
// visible; only the body is withheld.
func TestFourxxResponseBodyIsNotLogged(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusConflict,
		http.StatusRequestEntityTooLarge,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			out := dispatchOneAttempt(t, status)

			assert.NotContains(t, out, bodySentinel, "a 4xx body must never be logged")
			assert.NotContains(t, out, `"response_body"`, "the response_body key is reserved for bodies we do log")

			// The line itself must still be there, and still useful.
			assert.Contains(t, out, fmt.Sprintf(`"status":%d`, status))
			assert.Contains(t, out, `"room":"alerts-critical"`)
			assert.Contains(t, out, `"provider":"google_chat"`)
			assert.Contains(t, out, `"attempt":1`)
			assert.Contains(t, out, `"will_retry":false`)
			assert.Contains(t, out, `"body_withheld"`, "the line must say why there is no body")
			assertNoCredential(t, out)
		})
	}
}

// TestRetryableResponseBodyIsLogged is the other half. 429 and 5xx are Google
// describing its own state — an exhausted quota reads differently from a
// backend that is down — and that difference is the fact the 2026-10-28
// investigation lacked.
func TestRetryableResponseBodyIsLogged(t *testing.T) {
	for _, status := range []int{
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			out := dispatchOneAttempt(t, status)

			assert.Contains(t, out, bodySentinel, "a 429 or 5xx body must be logged")
			assert.Contains(t, out, `"response_body"`)
			assert.NotContains(t, out, `"body_withheld"`)

			assert.Contains(t, out, fmt.Sprintf(`"status":%d`, status))
			assertNoCredential(t, out)
		})
	}
}

// TestBodyIsSafeToLogBoundary pins the predicate itself, including the statuses
// either side of each edge, so a future edit cannot widen it by accident.
func TestBodyIsSafeToLogBoundary(t *testing.T) {
	for status, want := range map[int]bool{
		200: false, 204: false,
		400: false, 401: false, 403: false, 404: false, 413: false, 422: false,
		428: false, 429: true, 430: false, 451: false,
		499: false, 500: true, 502: true, 503: true, 504: true, 599: true,
	} {
		assert.Equalf(t, want, bodyIsSafeToLog(status), "bodyIsSafeToLog(%d)", status)
	}
}
