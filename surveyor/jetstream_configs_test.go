// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package surveyor

import (
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"

	st "github.com/nats-io/nats-surveyor/test"
	"github.com/prometheus/client_golang/prometheus"
	ptu "github.com/prometheus/client_golang/prometheus/testutil"
)

// newSubjectTestStream starts a JetStream server holding a stream named "events"
// with one message on events.a, two on events.b and three on events.c.
func newSubjectTestStream(t *testing.T) (nats.JetStreamContext, func()) {
	t.Helper()

	srv := st.NewJetStreamServer(t)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		srv.Shutdown()
		t.Fatalf("nats connect error: %s", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		srv.Shutdown()
		t.Fatalf("jetstream context error: %s", err)
	}

	// The test server's JetStream store directory outlives the process, so a
	// stream left behind by an earlier run would keep accumulating messages and
	// make the expected counts drift.
	_ = js.DeleteStream("events")

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "events",
		Subjects: []string{"events.>"},
	}); err != nil {
		nc.Close()
		srv.Shutdown()
		t.Fatalf("add stream error: %s", err)
	}

	for subject, count := range map[string]int{"events.a": 1, "events.b": 2, "events.c": 3} {
		for i := 0; i < count; i++ {
			if _, err := js.Publish(subject, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
				nc.Close()
				srv.Shutdown()
				t.Fatalf("publish error: %s", err)
			}
		}
	}

	return js, func() {
		_ = js.DeleteStream("events")
		nc.Close()
		srv.Shutdown()
	}
}

func newSubjectTestListener(t *testing.T, opts JSConfigListenerOptions) (*jsConfigListListener, *JSStreamConfigMetrics) {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	metrics := NewJetStreamConfigListMetrics(prometheus.NewRegistry(), nil)

	return NewJetStreamConfigListener(nil, logger, metrics, opts), metrics
}

// streamInfo fetches the stream the way the poll loop does, via STREAM.LIST, so
// the test exercises the same StreamInfo the handlers see in production —
// notably one without per-subject state.
func streamInfo(t *testing.T, js nats.JetStreamContext, name string) *nats.StreamInfo {
	t.Helper()

	for info := range js.Streams() {
		if info.Config.Name == name {
			return info
		}
	}
	t.Fatalf("stream %q not found", name)
	return nil
}

func TestJetStreamConfigs_StreamSequences(t *testing.T) {
	js, shutdown := newSubjectTestStream(t)
	defer shutdown()

	listener, metrics := newSubjectTestListener(t, JSConfigListenerOptions{})
	listener.StreamHandler(streamInfo(t, js, "events"))

	labels := prometheus.Labels{"stream_name": "events"}

	// Six messages published, so the newest carries sequence 6 and the oldest 1.
	if got := ptu.ToFloat64(metrics.jsStreamStateLastSeq.With(labels)); got != 6 {
		t.Fatalf("last sequence: expected 6, got %v", got)
	}
	if got := ptu.ToFloat64(metrics.jsStreamStateFirstSeq.With(labels)); got != 1 {
		t.Fatalf("first sequence: expected 1, got %v", got)
	}
}

func TestJetStreamConfigs_Subjects(t *testing.T) {
	js, shutdown := newSubjectTestStream(t)
	defer shutdown()

	listener, metrics := newSubjectTestListener(t, JSConfigListenerOptions{Subjects: true})
	listener.SubjectsHandler(js, streamInfo(t, js, "events"))

	expected := map[string]float64{"events.a": 1, "events.b": 2, "events.c": 3}
	for subject, want := range expected {
		got := ptu.ToFloat64(metrics.jsStreamSubjectMsgs.With(prometheus.Labels{
			"stream_name": "events",
			"subject":     subject,
		}))
		if got != want {
			t.Fatalf("subject %q: expected %v messages, got %v", subject, want, got)
		}
	}

	if got := ptu.CollectAndCount(metrics.jsStreamSubjectMsgs); got != len(expected) {
		t.Fatalf("expected %d subject series, got %d", len(expected), got)
	}

	skipped := ptu.ToFloat64(metrics.jsStreamSubjectsSkipped.With(prometheus.Labels{"stream_name": "events"}))
	if skipped != 0 {
		t.Fatalf("expected stream not to be skipped, got %v", skipped)
	}
}

// Per-subject collection is opt-in because it costs an extra request per stream.
func TestJetStreamConfigs_SubjectsDisabledByDefault(t *testing.T) {
	js, shutdown := newSubjectTestStream(t)
	defer shutdown()

	listener, metrics := newSubjectTestListener(t, JSConfigListenerOptions{})
	listener.SubjectsHandler(js, streamInfo(t, js, "events"))

	if got := ptu.CollectAndCount(metrics.jsStreamSubjectMsgs); got != 0 {
		t.Fatalf("expected no subject series when disabled, got %d", got)
	}
}

// A stream over the cap must be skipped whole and say so, rather than emit a
// silently truncated subset of its subjects.
func TestJetStreamConfigs_SubjectsCap(t *testing.T) {
	js, shutdown := newSubjectTestStream(t)
	defer shutdown()

	listener, metrics := newSubjectTestListener(t, JSConfigListenerOptions{Subjects: true, SubjectsMax: 2})
	listener.SubjectsHandler(js, streamInfo(t, js, "events"))

	if got := ptu.CollectAndCount(metrics.jsStreamSubjectMsgs); got != 0 {
		t.Fatalf("expected no subject series past the cap, got %d", got)
	}

	skipped := ptu.ToFloat64(metrics.jsStreamSubjectsSkipped.With(prometheus.Labels{"stream_name": "events"}))
	if skipped != 1 {
		t.Fatalf("expected the skip to be reported, got %v", skipped)
	}
}

func TestJetStreamConfigs_SubjectsAllowlist(t *testing.T) {
	js, shutdown := newSubjectTestStream(t)
	defer shutdown()

	listener, metrics := newSubjectTestListener(t, JSConfigListenerOptions{
		Subjects:       true,
		SubjectStreams: []string{"other"},
	})
	listener.SubjectsHandler(js, streamInfo(t, js, "events"))

	if got := ptu.CollectAndCount(metrics.jsStreamSubjectMsgs); got != 0 {
		t.Fatalf("expected no subject series for a stream outside the allowlist, got %d", got)
	}
	// A stream that was never considered is not a skipped stream.
	if got := ptu.CollectAndCount(metrics.jsStreamSubjectsSkipped); got != 0 {
		t.Fatalf("expected no skip series for a stream outside the allowlist, got %d", got)
	}
}

func TestJetStreamConfigs_ListenerDefaults(t *testing.T) {
	listener, _ := newSubjectTestListener(t, JSConfigListenerOptions{})

	if listener.opts.ScrapeInterval != DefaultScrapeInterval {
		t.Fatalf("expected default scrape interval %v, got %v", DefaultScrapeInterval, listener.opts.ScrapeInterval)
	}
	if listener.opts.SubjectsMax != DefaultSubjectsMaxPerStream {
		t.Fatalf("expected default subjects cap %d, got %d", DefaultSubjectsMaxPerStream, listener.opts.SubjectsMax)
	}
	if listener.subjectStreams != nil {
		t.Fatalf("expected an empty allowlist to mean all streams, got %v", listener.subjectStreams)
	}
}
