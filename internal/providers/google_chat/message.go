package google_chat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"unicode/utf8"

	retryablehttp "github.com/hashicorp/go-retryablehttp"
	chatv1 "google.golang.org/api/chat/v1"
)

const maxMsgSize = 4096

// prepareMessage renders text and CardsV2 independently. Only text is split
// to maxMsgSize bytes; CardsV2 has no verified size limit in this client.
func (m *GoogleChatManager) prepareMessage(alert any) ([]chatv1.Message, error) {
	var toText, toCard bytes.Buffer

	if err := m.msgTmpl.Execute(&toText, alert); err != nil {
		m.lo.Error("error parsing values in template", "error", err)
		return nil, err
	}
	if m.msgTmpl.Lookup("cardsV2") != nil {
		if err := m.msgTmpl.ExecuteTemplate(&toCard, "cardsV2", alert); err != nil {
			m.lo.Error("error parsing values in template", "error", err)
			return nil, err
		}
	}

	messages := make([]chatv1.Message, 0)
	if toText.Len() > 0 {
		for _, text := range splitText(withLegacyNewline(toText.String()), maxMsgSize) {
			messages = append(messages, chatv1.Message{Text: text})
		}
	}
	if toCard.Len() > 0 {
		var card chatv1.CardWithId
		if err := json.Unmarshal(toCard.Bytes(), &card); err != nil {
			m.lo.Error("error unmarshalling card message", "error", err)
			return nil, err
		}
		if len(messages) == 0 {
			messages = append(messages, chatv1.Message{})
		}
		messages[len(messages)-1].CardsV2 = []*chatv1.CardWithId{&card}
	}
	return messages, nil
}

// withLegacyNewline preserves the historic trailing newline without creating
// a newline-only chunk when rendered text is exactly a message boundary.
func withLegacyNewline(text string) string {
	if len(text)%maxMsgSize == 0 {
		return text
	}
	return text + "\n"
}

func splitText(text string, limit int) []string {
	if text == "" {
		return nil
	}
	chunks := make([]string, 0, (len(text)+limit-1)/limit)
	start, size := 0, 0
	for index, r := range text {
		runeSize := utf8.RuneLen(r)
		if size > 0 && size+runeSize > limit {
			chunks = append(chunks, text[start:index])
			start, size = index, 0
		}
		size += runeSize
	}
	if start < len(text) {
		chunks = append(chunks, text[start:])
	}
	return chunks
}

// sendMessage pushes out a notification to Google Chat space.
func (m *GoogleChatManager) sendMessage(ctx context.Context, msg chatv1.Message, threadKey string, threaded bool) error {
	out, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encoding Google Chat message")
	}

	u, err := url.Parse(m.endpoint)
	if err != nil {
		return fmt.Errorf("building Google Chat request")
	}
	q := u.Query()
	q.Set("messageReplyOption", "MESSAGE_REPLY_OPTION_UNSPECIFIED")
	if threaded {
		q.Set("messageReplyOption", "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD")
		q.Set("threadKey", threadKey)
	}
	u.RawQuery = q.Encode()

	hasCard := len(msg.CardsV2) > 0
	m.lo.Debug("sending alert", "threaded", threaded, "has_cards_v2", hasCard, "payload_bytes", len(out))
	// The retry policy records each attempt's status here. retryablehttp hands
	// CheckRetry the request's context (client.go:725), and it returns a nil
	// response once it gives up (client.go:830-832), so this is the only way
	// the status of a retried failure survives to the error below.
	attempt := &chatAttempt{}
	req, err := retryablehttp.NewRequestWithContext(withChatAttempt(ctx, attempt), http.MethodPost, u.String(), bytes.NewReader(out))
	if err != nil {
		return fmt.Errorf("building Google Chat request")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		// Text unchanged: retryablehttp's own error names the full request URL,
		// so it is still neither wrapped nor logged. Only the status travels.
		return &chatSendError{msg: "sending Google Chat request", status: attempt.status}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return &chatSendError{
			msg:    fmt.Sprintf("google chat returned status %d", resp.StatusCode),
			status: resp.StatusCode,
		}
	}
	return nil
}

// chatSendError carries the HTTP status Google Chat returned alongside the
// error, so the dispatch error counter can label it. The error text is
// unchanged and still says nothing about the request.
type chatSendError struct {
	msg    string
	status int
}

func (e *chatSendError) Error() string { return e.msg }

// Status returns the HTTP status Google Chat returned, or 0 when no response
// ever arrived.
func (e *chatSendError) Status() int { return e.status }

// chatStatusLabel renders err's status for the `status` label on
// calert_alerts_dispatched_errors_total. "none" means no response arrived at
// all — a timeout or a connection error — which is a different fact from any
// status Google returned. Cardinality is the handful of statuses Chat returns
// times the configured rooms.
func chatStatusLabel(err error) string {
	var sendErr *chatSendError
	if errors.As(err, &sendErr) && sendErr.status > 0 {
		return strconv.Itoa(sendErr.status)
	}
	return "none"
}
