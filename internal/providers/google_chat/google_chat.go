package google_chat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
	"time"

	retryablehttp "github.com/hashicorp/go-retryablehttp"
	"github.com/mr-karan/calert/internal/metrics"
	"github.com/mr-karan/calert/internal/providers"
	alertmgrtmpl "github.com/prometheus/alertmanager/template"
	"github.com/redis/go-redis/v9"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type GoogleChatManager struct {
	lo              *slog.Logger
	metrics         *metrics.Manager
	endpoint        string
	room            string
	client          *retryablehttp.Client
	msgTmpl         *template.Template
	dryRun          bool
	threadedReplies bool
	threadingMode   string
	threadTTL       time.Duration
	dedupWindow     time.Duration
	maxAlertsPerMsg int
	groupStore      groupStateStore
}

const (
	// ThreadingModeAlert is the legacy behaviour: one deterministic thread per
	// alert lifecycle.
	ThreadingModeAlert = "alert"
	// ThreadingModeGroup threads by Alertmanager group: one aggregated
	// message per webhook payload, posted into a deterministic thread.
	ThreadingModeGroup = "group"
)

type GoogleChatOpts struct {
	Log             *slog.Logger
	Metrics         *metrics.Manager
	DryRun          bool
	MaxIdleConn     int
	Timeout         time.Duration
	ProxyURL        string
	Endpoint        string
	Room            string
	Template        string
	ThreadTTL       time.Duration
	ThreadedReplies bool
	ThreadingMode   string
	DedupWindow     time.Duration
	MaxAlertsPerMsg int
	RetryMax        int
	RetryWaitMin    time.Duration
	RetryWaitMax    time.Duration

	// Optional shared dedup state for active-active deployments. When
	// RedisAddress is empty, group state stays in memory (default).
	RedisAddress   string
	RedisPassword  string
	RedisDB        int
	RedisKeyPrefix string
}

// NewGoogleChat initializes a Google Chat provider object.
func NewGoogleChat(opts GoogleChatOpts) (*GoogleChatManager, error) {
	transport := &http.Transport{
		MaxIdleConnsPerHost: opts.MaxIdleConn,
	}

	// Add a proxy to make upstream requests if specified in config.
	if opts.ProxyURL != "" {
		u, err := url.Parse(opts.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("error parsing proxy URL: %s", err)
		}
		transport.Proxy = http.ProxyURL(u)
	}

	// Declared here so the CheckRetry policy below can name the room and the
	// provider it is reporting for. The closure captures the variable, not its
	// value, and it only ever runs through mgr.client, which exists after mgr
	// is assigned below.
	var mgr *GoogleChatManager

	// Initialise a retryable HTTP Client for communicating with the G-Chat APIs.
	client := retryablehttp.NewClient()
	client.RetryMax = opts.RetryMax
	client.RetryWaitMin = opts.RetryWaitMin
	client.RetryWaitMax = opts.RetryWaitMax
	client.HTTPClient.Timeout = opts.Timeout
	client.HTTPClient.Transport = transport
	// retryablehttp logs full request URLs, including webhook tokens.
	client.Logger = nil

	// Custom CheckRetry policy that also retries on 429 (Too Many Requests).
	// This is important for Google Chat API which rate limits requests.
	client.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		// First, check the default retry policy.
		shouldRetry, checkErr := retryablehttp.DefaultRetryPolicy(ctx, resp, err)

		// Additionally retry on 429 Too Many Requests.
		if !shouldRetry && resp != nil && resp.StatusCode == http.StatusTooManyRequests {
			shouldRetry, checkErr = true, nil
		}

		// Report what Google Chat returned. This is the only place that can:
		// once the retries are exhausted retryablehttp drains the response
		// body and returns a nil response (go-retryablehttp client.go:830-832),
		// so the status never reaches sendMessage and the one branch that does
		// print a status (message.go) is unreachable for every retried status.
		reportChatResponse(ctx, mgr, resp, shouldRetry, client.RetryMax)

		if shouldRetry {
			return true, checkErr
		}

		return false, nil
	}

	// Initialise message template functions.
	templateFuncMap := template.FuncMap{
		"Title": func(v any) string {
			s := fmt.Sprintf("%v", v)
			titleCaser := cases.Title(language.English)
			return titleCaser.String(s)
		},
		"toUpper": func(v any) string {
			return strings.ToUpper(fmt.Sprintf("%v", v))
		},
		"toLower": func(v any) string {
			return strings.ToLower(fmt.Sprintf("%v", v))
		},
		"Contains": func(s, substr any) bool {
			return strings.Contains(fmt.Sprintf("%v", s), fmt.Sprintf("%v", substr))
		},
		"HasPrefix": func(s, prefix any) bool {
			return strings.HasPrefix(fmt.Sprintf("%v", s), fmt.Sprintf("%v", prefix))
		},
		"HasSuffix": func(s, suffix any) bool {
			return strings.HasSuffix(fmt.Sprintf("%v", s), fmt.Sprintf("%v", suffix))
		},
		"Replace": func(s, old, new any) string {
			return strings.ReplaceAll(fmt.Sprintf("%v", s), fmt.Sprintf("%v", old), fmt.Sprintf("%v", new))
		},
		"TrimSpace": func(v any) string {
			return strings.TrimSpace(fmt.Sprintf("%v", v))
		},
		"Default": func(defaultVal, val any) any {
			if val == nil || fmt.Sprintf("%v", val) == "" {
				return defaultVal
			}
			return val
		},
		"reReplaceAll": func(pattern, repl, text string) string {
			re := regexp.MustCompile(pattern)
			return re.ReplaceAllString(text, repl)
		},
		"CurrentTime": func(location ...string) string {
			if len(location) == 0 || location[0] == "" {
				return time.Now().Format("2006-01-02 15:04:05 MST")
			}
			loc, err := time.LoadLocation(location[0])
			if err != nil {
				return fmt.Sprintf("Error loading timezone: %v", err)
			}
			return time.Now().In(loc).Format("2006-01-02 15:04:05 MST")
		},
		"ConvertTZ": func(t time.Time, location string) string {
			loc, err := time.LoadLocation(location)
			if err != nil {
				return fmt.Sprintf("Error loading timezone: %v", err)
			}
			return t.In(loc).Format("2006-01-02 15:04:05 MST")
		},
		"DurationSince": func(t time.Time) string {
			d := time.Since(t)
			h := int(d.Hours())
			m := int(d.Minutes()) % 60
			s := int(d.Seconds()) % 60
			return fmt.Sprintf("%dh %dm %ds", h, m, s)
		},
		"json": func(v any) string {
			encoded, err := json.Marshal(v)
			if err != nil {
				return `null`
			}
			return string(encoded)
		},
	}

	opts.Template = templateForMode(opts.Template, opts.ThreadingMode)

	// Load the template.
	tmpl, err := template.New(filepath.Base(opts.Template)).Funcs(templateFuncMap).ParseFiles(opts.Template)
	if err != nil {
		return nil, err
	}

	mgr = &GoogleChatManager{
		lo:              opts.Log,
		metrics:         opts.Metrics,
		client:          client,
		endpoint:        opts.Endpoint,
		room:            opts.Room,
		msgTmpl:         tmpl,
		dryRun:          opts.DryRun,
		threadedReplies: opts.ThreadedReplies,
		threadingMode:   opts.ThreadingMode,
		threadTTL:       opts.ThreadTTL,
		dedupWindow:     opts.DedupWindow,
		maxAlertsPerMsg: opts.MaxAlertsPerMsg,
	}
	if mgr.threadingMode == "" {
		mgr.threadingMode = ThreadingModeAlert
	}
	if mgr.threadTTL < 0 {
		return nil, fmt.Errorf("thread_ttl must not be negative")
	}
	if mgr.threadTTL == 0 {
		mgr.threadTTL = 12 * time.Hour
	}
	if mgr.dedupWindow < 0 {
		return nil, fmt.Errorf("dedup_window must not be negative")
	}
	if mgr.threadingMode != ThreadingModeAlert && mgr.threadingMode != ThreadingModeGroup {
		return nil, fmt.Errorf("invalid threading_mode %q: must be %q or %q", mgr.threadingMode, ThreadingModeAlert, ThreadingModeGroup)
	}
	if mgr.threadingMode == ThreadingModeGroup && mgr.dedupWindow == 0 {
		mgr.dedupWindow = 2 * time.Minute
	}
	if mgr.threadingMode == ThreadingModeGroup && mgr.maxAlertsPerMsg <= 0 {
		return nil, fmt.Errorf("max_alerts_per_message must be greater than zero in group mode")
	}
	// Group mode is threaded by definition (one thread per alert group), so it
	// ignores threaded_replies. Force it on and tell the operator, rather than
	// silently honouring a flag that can't apply.
	if mgr.threadingMode == ThreadingModeGroup && !mgr.threadedReplies {
		opts.Log.Warn("threaded_replies is ignored in group threading mode; threading is always on", "room", mgr.room)
		mgr.threadedReplies = true
	}
	if mgr.threadingMode != ThreadingModeGroup {
		return mgr, nil
	}

	// Select the group dedup store. With Redis configured, both calert
	// instances share state; otherwise state is per-process in memory.
	if opts.RedisAddress != "" {
		client := redis.NewClient(&redis.Options{
			Addr:     opts.RedisAddress,
			Password: opts.RedisPassword,
			DB:       opts.RedisDB,
		})
		mgr.groupStore = newRedisStore(opts.Log, client, opts.RedisKeyPrefix, mgr.threadTTL)
		opts.Log.Info("group dedup state shared via redis", "room", opts.Room)
	} else {
		mgr.groupStore = newMemoryStore(opts.Log, mgr.threadTTL)
	}

	return mgr, nil
}

func templateForMode(path, mode string) string {
	if mode == ThreadingModeGroup && path == "static/message.tmpl" {
		return "static/message-group.tmpl"
	}
	return path
}

// Push accepts an Alertmanager webhook payload and dispatches notifications
// to the Webhook API endpoint, threading either per alert (legacy) or per
// group depending on the configured threading mode.
func (m *GoogleChatManager) Push(ctx context.Context, payload providers.WebhookPayload) error {
	if m.threadingMode == ThreadingModeGroup {
		return m.pushGroup(ctx, payload)
	}
	return m.pushAlerts(ctx, payload.Alerts)
}

// pushGroup posts one aggregated message per webhook payload into the
// group's deterministic thread.
func (m *GoogleChatManager) pushGroup(ctx context.Context, payload providers.WebhookPayload) error {
	now := time.Now()
	hash := stateHash(payload)
	threadKey := threadKeyForGroup(m.room, payload.GroupKey)

	m.lo.Info("dispatching group notification to google chat", "group_key", payload.GroupKey, "count", len(payload.Alerts))

	previous, duplicate, err := m.groupStore.inspect(ctx, m.room, payload.GroupKey, hash, now, m.dedupWindow)
	if err != nil {
		m.lo.Warn("group dedup store unavailable, posting anyway", "error", err, "group_key", payload.GroupKey)
		m.metrics.Increment(fmt.Sprintf(`group_dedup_store_errors_total{provider="%s", room="%s"}`, m.ID(), m.Room()))
		previous = groupState{}
	} else if duplicate {
		m.metrics.Increment(fmt.Sprintf(`alerts_deduplicated_total{provider="%s", room="%s"}`, m.ID(), m.Room()))
		return nil
	}

	tmplCtx := buildGroupContext(payload, threadKey, m.maxAlertsPerMsg, previous.lastStatuses)
	if len(tmplCtx.Alerts) == 0 && tmplCtx.TruncatedAlerts == 0 {
		return nil
	}

	msgs, err := m.prepareMessage(tmplCtx)
	if err != nil {
		m.metrics.Increment(fmt.Sprintf(`alerts_dispatched_errors_total{provider="%s", room="%s", reason="preparing"}`, m.ID(), m.Room()))
		return err
	}
	if m.dryRun {
		m.lo.Info("dry_run is enabled for this room. skipping pushing notification", "room", m.Room())
		return nil
	}

	for _, msg := range msgs {
		if err := m.sendMessage(ctx, msg, threadKey, true); err != nil {
			m.metrics.Increment(fmt.Sprintf(`alerts_dispatched_errors_total{provider="%s", room="%s", reason="sending", status="%s"}`, m.ID(), m.Room(), chatStatusLabel(err)))
			return err
		}
	}

	completedAt := time.Now()
	if err := m.groupStore.commit(ctx, m.room, payload.GroupKey, groupState{
		lastHash:     hash,
		lastPost:     completedAt,
		lastStatuses: statusesOf(payload.Alerts),
	}); err != nil {
		m.lo.Warn("saving group dedup state failed after delivery", "error", err, "group_key", payload.GroupKey)
		m.metrics.Increment(fmt.Sprintf(`group_dedup_store_errors_total{provider="%s", room="%s"}`, m.ID(), m.Room()))
	}
	m.metrics.IncrementBy(fmt.Sprintf(`alerts_dispatched_total{provider="%s", room="%s"}`, m.ID(), m.Room()), len(payload.Alerts))
	m.metrics.Duration(fmt.Sprintf(`alerts_dispatched_duration_seconds{provider="%s", room="%s"}`, m.ID(), m.Room()), now)
	return nil
}

// pushAlerts is the legacy per-alert dispatch path.
func (m *GoogleChatManager) pushAlerts(ctx context.Context, alerts []alertmgrtmpl.Alert) error {
	m.lo.Info("dispatching alerts to google chat", "count", len(alerts))
	for _, alert := range alerts {
		now := time.Now()
		msgs, err := m.prepareMessage(alert)
		if err != nil {
			m.metrics.Increment(fmt.Sprintf(`alerts_dispatched_errors_total{provider="%s", room="%s", reason="preparing"}`, m.ID(), m.Room()))
			return err
		}
		if m.dryRun {
			m.lo.Info("dry_run is enabled for this room. skipping pushing notification", "room", m.Room())
			continue
		}
		threadKey := threadKeyForAlert(m.room, alert.Fingerprint, alert.StartsAt)
		for _, msg := range msgs {
			if err := m.sendMessage(ctx, msg, threadKey, m.threadedReplies); err != nil {
				m.metrics.Increment(fmt.Sprintf(`alerts_dispatched_errors_total{provider="%s", room="%s", reason="sending", status="%s"}`, m.ID(), m.Room(), chatStatusLabel(err)))
				return err
			}
		}
		m.metrics.Increment(fmt.Sprintf(`alerts_dispatched_total{provider="%s", room="%s"}`, m.ID(), m.Room()))
		m.metrics.Duration(fmt.Sprintf(`alerts_dispatched_duration_seconds{provider="%s", room="%s"}`, m.ID(), m.Room()), now)
	}
	return nil
}

// Room returns the name of room for which this provider is configured.
func (m *GoogleChatManager) Room() string {
	return m.room
}

// ID returns the provider name.
func (m *GoogleChatManager) ID() string {
	return "google_chat"
}

// chatResponseBodyLimit caps how much of Google Chat's response is reported.
// Google's error JSON names the cause — a quota that is exhausted reads
// differently from an argument it rejected — well inside this, and the cap
// stops one rejected dispatch from filling a log line.
const chatResponseBodyLimit = 512

// reportChatResponse records what Google Chat returned for one attempt: it logs
// the status and Google's response body, and it stores the status on the
// request's chatAttempt so the dispatch error counter can label it.
//
// It stays quiet on a 200. A delivered alert has no status to explain, and the
// control for this is a successful dispatch whose log carries no status at all.
//
// What must never be logged, and why this function takes no URL, no request
// body and no error value:
//
//   - The request URL is a credential. A Google Chat incoming webhook carries
//     its key and its token as query parameters, and retryablehttp's redactURL
//     (client.go:928-938) masks only the userinfo password, so even the
//     "redacted" URL prints both. google_chat.go sets client.Logger to nil for
//     exactly this reason; that line is a control, not an oversight.
//   - The err that CheckRetry receives is a *url.Error, whose message is built
//     from that same URL. So it is not logged either, not even its text.
//   - The request body and any alert label or annotation value are not logged.
//     Nothing in this estate screens alert labels for PHI, and the Chat card is
//     built from them.
//
// Google's own response is what is safe to report. The rule in one line: log
// what Google said, never what we sent. The same rule, with the same reasoning,
// is at livekit-agent-watchdog/main.py:352-366 in quadrivia-ai/devops.
func reportChatResponse(ctx context.Context, m *GoogleChatManager, resp *http.Response, shouldRetry bool, retryMax int) {
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}

	attempt := 0
	if a := chatAttemptFrom(ctx); a != nil {
		a.count++
		attempt = a.count
		// Only a real response sets the status, so the last status Google
		// returned survives a later attempt that never reached it at all.
		if status != 0 {
			a.status = status
		}
	}

	// shouldRetry on its own is not whether a retry happens. retryablehttp
	// calls CheckRetry before it checks the retry budget (client.go:755-764),
	// so on the last attempt the policy still answers "retry" and then the loop
	// gives up. Mirroring that budget check here is what makes the line that
	// loses the alert say so.
	willRetry := shouldRetry && attempt > 0 && attempt <= retryMax

	if m == nil || m.lo == nil || status == http.StatusOK {
		return
	}

	// An attempt that will be retried is a warning; the one that gives up is
	// the error that loses the alert.
	level := slog.LevelWarn
	if !willRetry {
		level = slog.LevelError
	}
	m.lo.Log(ctx, level, "google chat did not accept the alert",
		"status", status,
		"room", m.Room(),
		"provider", m.ID(),
		"attempt", attempt,
		"will_retry", willRetry,
		"response_body", readChatResponseBody(resp),
	)
}

// readChatResponseBody returns at most chatResponseBodyLimit bytes of Google's
// response and puts them back, so every later reader sees the body it would
// have seen. The real Closer is kept rather than wrapped in io.NopCloser:
// retryablehttp drains and closes this body between attempts
// (client.go:766-769) and sendMessage closes it at the end, and a no-op Close
// would leak the connection on every rejected dispatch.
func readChatResponseBody(resp *http.Response) string {
	if resp == nil || resp.Body == nil {
		return ""
	}
	orig := resp.Body
	buf, err := io.ReadAll(io.LimitReader(orig, chatResponseBodyLimit))
	if err != nil && len(buf) == 0 {
		return ""
	}
	resp.Body = replayBody{
		Reader: io.MultiReader(bytes.NewReader(buf), orig),
		Closer: orig,
	}
	return string(buf)
}

// replayBody serves bytes already read from a response body again, while Close
// still closes the real body.
type replayBody struct {
	io.Reader
	io.Closer
}

// chatAttempt records what Google Chat returned across the retries of one
// request. reportChatResponse fills it in and sendMessage reads it, because
// retryablehttp returns a nil response once it gives up (client.go:830-832) and
// the status would otherwise be lost at the point the error is counted.
//
// One value per request, reached through the request context. retryablehttp
// drives every attempt of a request from the caller's goroutine, so the writes
// here and the read in sendMessage never overlap.
type chatAttempt struct {
	count  int
	status int
}

type chatAttemptKey struct{}

// withChatAttempt attaches a per-request chatAttempt to ctx.
func withChatAttempt(ctx context.Context, a *chatAttempt) context.Context {
	return context.WithValue(ctx, chatAttemptKey{}, a)
}

// chatAttemptFrom returns the request's chatAttempt, or nil when the caller did
// not attach one.
func chatAttemptFrom(ctx context.Context) *chatAttempt {
	a, _ := ctx.Value(chatAttemptKey{}).(*chatAttempt)
	return a
}
