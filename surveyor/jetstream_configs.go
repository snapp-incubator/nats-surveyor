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
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	//JSStreamList          = `$JS.API.STREAM.LIST`
	streamConfigLabels           = []string{"discard_policy", "storage_type", "replica_number", "stream_name"}
	consumerConfigLabels         = []string{"stream_name", "max_pending_ack", "ack_policy", "is_pull", "consumer_name"}
	streamRaftInfoLabels         = []string{"stream_name", "leader", "replica_count"}
	streamRaftPeerInfoLabels     = []string{"stream_name", "peer_name", "offline", "current", "leader"}
	consumerRaftInfoLabels       = []string{"consumer_name", "leader", "replica_count", "stream_name"}
	consumerStateLabels          = []string{"consumer_name", "stream_name"}
	consumerRaftPeerInfoLabels   = []string{"stream_name", "consumer_name", "peer_name", "offline", "current", "leader"}
	streamReplicationLagLabels   = []string{"stream_name", "peer_name"}
	consumerReplicationLagLabels = []string{"stream_name", "consumer_name", "peer_name"}
	DefaultScrapeInterval        = 10 * time.Second
	streamLabel                  = []string{"stream_name"}
	streamSubjectLabels          = []string{"stream_name", "subject"}
	consumerStreamLabel          = []string{"consumer_name", "stream_name"}

	//DefaultListenerID     = "default_listener"
)

// DefaultSubjectsMaxPerStream bounds per-subject collection. A stream whose
// subject space is a wildcard has an unbounded subject count, and every subject
// becomes its own Prometheus series, so streams above this are skipped rather
// than partially reported.
const DefaultSubjectsMaxPerStream = 100

type JSStreamConfigMetrics struct {
	jsStreamConfig           *prometheus.GaugeVec
	jsStreamRaftInfo         *prometheus.GaugeVec
	jsStreamRaftPeerInfo     *prometheus.GaugeVec
	jsStreamReplicationLag   *prometheus.GaugeVec
	jsStreamStateConsumerNum *prometheus.GaugeVec
	jsStreamStateDeletedNum  *prometheus.GaugeVec
	jsStreamStateSubjectNum  *prometheus.GaugeVec
	jsStreamStateFirstSeq    *prometheus.GaugeVec
	jsStreamStateLastSeq     *prometheus.GaugeVec
	jsStreamSubjectMsgs      *prometheus.GaugeVec
	jsStreamSubjectsSkipped  *prometheus.GaugeVec

	jsConsumerConfig                *prometheus.GaugeVec
	jsConsumerState                 *prometheus.GaugeVec
	jsConsumerRaftInfo              *prometheus.GaugeVec
	jsConsumerRaftPeerInfo          *prometheus.GaugeVec
	jsConsumerReplicationLag        *prometheus.GaugeVec
	jsConsumerAckPendingNum         *prometheus.GaugeVec
	jsConsumerPendingNum            *prometheus.GaugeVec
	jsConsumerRedeliveredNum        *prometheus.GaugeVec
	jsConsumerWaitingNum            *prometheus.GaugeVec
	jsConsumerLastAckFloorConsumer  *prometheus.GaugeVec
	jsConsumerLastAckFloorStream    *prometheus.GaugeVec
	jsConsumerLastDeliveredConsumer *prometheus.GaugeVec
	jsConsumerLastDeliveredStream   *prometheus.GaugeVec

	jsStreamLimitMaxMsgs      *prometheus.GaugeVec
	jsStreamLimitMaxMsgsPer   *prometheus.GaugeVec
	jsStreamLimitMaxBytes     *prometheus.GaugeVec
	jsStreamLimitMaxAge       *prometheus.GaugeVec
	jsStreamLimitMaxMsgSize   *prometheus.GaugeVec
	jsStreamLimitMaxConsumers *prometheus.GaugeVec
}

func NewJetStreamConfigListMetrics(registry *prometheus.Registry, constLabels prometheus.Labels) *JSStreamConfigMetrics {
	metrics := &JSStreamConfigMetrics{
		// API Audit
		jsStreamConfig: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_configuration"),
			Help:        "Configurations for streams",
			ConstLabels: constLabels,
		}, streamConfigLabels),
		jsConsumerConfig: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_configuration"),
			Help:        "Configurations for consumer",
			ConstLabels: constLabels,
		}, consumerConfigLabels),
		jsConsumerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_state"),
			Help:        "state of consumer consumer",
			ConstLabels: constLabels,
		}, consumerStateLabels),
		jsStreamRaftInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_raft_info"),
			Help:        "raft info for streams",
			ConstLabels: constLabels,
		}, streamRaftInfoLabels),
		jsStreamRaftPeerInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_raft_peer_info"),
			Help:        "raft peer info for streams",
			ConstLabels: constLabels,
		}, streamRaftPeerInfoLabels),
		jsStreamStateConsumerNum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_consumer_number"),
			Help:        "consumer number of stream",
			ConstLabels: constLabels,
		}, streamLabel),
		jsStreamStateDeletedNum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_deleted_number"),
			Help:        "deleted number of stream",
			ConstLabels: constLabels,
		}, streamLabel),
		jsStreamStateSubjectNum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_subject_number"),
			Help:        "subject number for streams",
			ConstLabels: constLabels,
		}, streamLabel),
		jsStreamStateFirstSeq: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_first_sequence"),
			Help:        "Sequence number of the oldest message still held by the stream",
			ConstLabels: constLabels,
		}, streamLabel),
		jsStreamStateLastSeq: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: prometheus.BuildFQName("nats", "jetstream", "stream_last_sequence"),
			// Monotonic, so rate() over this is the stream's true publish rate.
			// The message count is not: retention ages messages out from under it.
			Help:        "Sequence number of the newest message published to the stream",
			ConstLabels: constLabels,
		}, streamLabel),
		jsStreamSubjectMsgs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_subject_messages"),
			Help:        "Messages currently held by the stream, per subject (requires per-subject collection)",
			ConstLabels: constLabels,
		}, streamSubjectLabels),
		jsStreamSubjectsSkipped: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_subjects_collection_skipped"),
			Help:        "1 when per-subject collection was skipped for this stream because it exceeds the subject cap",
			ConstLabels: constLabels,
		}, streamLabel),
		jsConsumerRaftInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_raft_info"),
			Help:        "raft info for consumer",
			ConstLabels: constLabels,
		}, consumerRaftInfoLabels),
		jsConsumerRaftPeerInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_raft_peer_info"),
			Help:        "raft peer info for consumer",
			ConstLabels: constLabels,
		}, consumerRaftPeerInfoLabels),
		jsStreamReplicationLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_replication_lag"),
			Help:        "replication lag of stream peers",
			ConstLabels: constLabels,
		}, streamReplicationLagLabels),
		jsConsumerReplicationLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_replication_lag"),
			Help:        "replication lag of consumer peers",
			ConstLabels: constLabels,
		}, consumerReplicationLagLabels),
		jsConsumerAckPendingNum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_ack_pending_number"),
			Help:        "pending ack number of consumer",
			ConstLabels: constLabels,
		}, consumerStreamLabel),
		jsConsumerPendingNum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_pending_number"),
			Help:        "pending number of consumer",
			ConstLabels: constLabels,
		}, consumerStreamLabel),
		jsConsumerRedeliveredNum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_redelivered_number"),
			Help:        "redelivered number of consumer",
			ConstLabels: constLabels,
		}, consumerStreamLabel),
		jsConsumerWaitingNum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_waiting_number"),
			Help:        "waiting number of consumer",
			ConstLabels: constLabels,
		}, consumerStreamLabel),
		jsConsumerLastAckFloorConsumer: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_last_ack_floor_consumer_seq"),
			Help:        "Consumer-scoped sequence number of the last acknowledged message",
			ConstLabels: constLabels,
		}, consumerStreamLabel),
		jsConsumerLastAckFloorStream: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_last_ack_floor_stream_seq"),
			Help:        "Stream-scoped sequence number of the last acknowledged message",
			ConstLabels: constLabels,
		}, consumerStreamLabel),
		jsConsumerLastDeliveredConsumer: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_last_delivered_consumer_seq"),
			Help:        "Consumer-scoped sequence number of the last delivered message",
			ConstLabels: constLabels,
		}, consumerStreamLabel),
		jsConsumerLastDeliveredStream: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_last_delivered_stream_seq"),
			Help:        "Stream-scoped sequence number of the last delivered message",
			ConstLabels: constLabels,
		}, consumerStreamLabel),
		jsStreamLimitMaxMsgs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_limit_max_msgs"),
			Help:        "Maximum number of messages allowed in the stream (-1 for unlimited)",
			ConstLabels: constLabels,
		}, streamLabel),
		jsStreamLimitMaxMsgsPer: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_limit_max_msgs_per_subject"),
			Help:        "Maximum number of messages per subject allowed in the stream (-1 for unlimited)",
			ConstLabels: constLabels,
		}, streamLabel),
		jsStreamLimitMaxBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_limit_max_bytes"),
			Help:        "Maximum total bytes allowed in the stream (-1 for unlimited)",
			ConstLabels: constLabels,
		}, streamLabel),
		jsStreamLimitMaxAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_limit_max_age_seconds"),
			Help:        "Maximum age of messages in seconds (0 for unlimited)",
			ConstLabels: constLabels,
		}, streamLabel),
		jsStreamLimitMaxMsgSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_limit_max_msg_size"),
			Help:        "Maximum message size in bytes (-1 for unlimited)",
			ConstLabels: constLabels,
		}, streamLabel),
		jsStreamLimitMaxConsumers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "stream_limit_max_consumers"),
			Help:        "Maximum number of consumers allowed (-1 for unlimited)",
			ConstLabels: constLabels,
		}, streamLabel),
	}

	registry.MustRegister(metrics.jsStreamConfig)
	registry.MustRegister(metrics.jsConsumerConfig)
	registry.MustRegister(metrics.jsConsumerState)
	registry.MustRegister(metrics.jsStreamRaftInfo)
	registry.MustRegister(metrics.jsStreamRaftPeerInfo)
	registry.MustRegister(metrics.jsStreamStateSubjectNum)
	registry.MustRegister(metrics.jsStreamStateDeletedNum)
	registry.MustRegister(metrics.jsStreamStateConsumerNum)
	registry.MustRegister(metrics.jsStreamStateFirstSeq)
	registry.MustRegister(metrics.jsStreamStateLastSeq)
	registry.MustRegister(metrics.jsStreamSubjectMsgs)
	registry.MustRegister(metrics.jsStreamSubjectsSkipped)

	registry.MustRegister(metrics.jsConsumerRaftInfo)
	registry.MustRegister(metrics.jsConsumerRaftPeerInfo)
	registry.MustRegister(metrics.jsStreamReplicationLag)
	registry.MustRegister(metrics.jsConsumerReplicationLag)
	registry.MustRegister(metrics.jsConsumerWaitingNum)
	registry.MustRegister(metrics.jsConsumerAckPendingNum)
	registry.MustRegister(metrics.jsConsumerPendingNum)
	registry.MustRegister(metrics.jsConsumerRedeliveredNum)
	registry.MustRegister(metrics.jsConsumerLastDeliveredConsumer)
	registry.MustRegister(metrics.jsConsumerLastDeliveredStream)
	registry.MustRegister(metrics.jsConsumerLastAckFloorConsumer)
	registry.MustRegister(metrics.jsConsumerLastAckFloorStream)

	registry.MustRegister(metrics.jsStreamLimitMaxMsgs)
	registry.MustRegister(metrics.jsStreamLimitMaxMsgsPer)
	registry.MustRegister(metrics.jsStreamLimitMaxBytes)
	registry.MustRegister(metrics.jsStreamLimitMaxAge)
	registry.MustRegister(metrics.jsStreamLimitMaxMsgSize)
	registry.MustRegister(metrics.jsStreamLimitMaxConsumers)

	return metrics
}

// JSConfigListenerOptions tunes the JetStream config/state poll loop.
type JSConfigListenerOptions struct {
	// ScrapeInterval is how often the stream/consumer poll loop runs.
	// Zero selects DefaultScrapeInterval.
	ScrapeInterval time.Duration

	// Subjects enables per-subject stream state collection. Off by default:
	// it costs one extra STREAM.INFO request per stream, and the server
	// assembles per-subject state in O(subjects) on the stream leader.
	Subjects bool

	// SubjectStreams restricts per-subject collection to these stream names.
	// Empty means every stream, still bounded by SubjectsMax.
	SubjectStreams []string

	// SubjectsMax skips per-subject collection for any stream reporting more
	// subjects than this. Zero selects DefaultSubjectsMaxPerStream.
	SubjectsMax int
}

// jsConfigListListener polls JetStream stream/consumer configuration and state on an
// interval and exposes the result as prometheus metrics.
type jsConfigListListener struct {
	sync.Mutex
	cancelLoop context.CancelFunc
	wg         sync.WaitGroup
	provider   ConnProvider
	logger     *logrus.Logger
	metrics    *JSStreamConfigMetrics
	opts       JSConfigListenerOptions
	// subjectStreams is the set form of opts.SubjectStreams; nil means "all streams".
	subjectStreams map[string]struct{}
	conn           Conn
	js             nats.JetStreamContext
}

func NewJetStreamConfigListener(
	provider ConnProvider,
	logger *logrus.Logger,
	metrics *JSStreamConfigMetrics,
	opts JSConfigListenerOptions,
) *jsConfigListListener {
	if opts.ScrapeInterval <= 0 {
		opts.ScrapeInterval = DefaultScrapeInterval
	}
	if opts.SubjectsMax <= 0 {
		opts.SubjectsMax = DefaultSubjectsMaxPerStream
	}

	var subjectStreams map[string]struct{}
	if len(opts.SubjectStreams) > 0 {
		subjectStreams = make(map[string]struct{}, len(opts.SubjectStreams))
		for _, name := range opts.SubjectStreams {
			subjectStreams[name] = struct{}{}
		}
	}

	return &jsConfigListListener{
		provider:       provider,
		logger:         logger,
		metrics:        metrics,
		opts:           opts,
		subjectStreams: subjectStreams,
	}
}

func (o *jsConfigListListener) gatherData(ctx context.Context, interval time.Duration) {
	defer o.wg.Done()
	ticker := time.NewTicker(interval)
	o.logger.Infoln("starting config list listener ticker")
	for {
		select {
		case <-ticker.C:
			// Snapshot js under the lock to avoid holding it across blocking NATS I/O.
			o.Lock()
			js := o.js
			o.Unlock()
			if js == nil || ctx.Err() != nil {
				return
			}
			for str := range js.Streams() {
				if ctx.Err() != nil {
					return
				}
				o.StreamHandler(str)
				o.SubjectsHandler(js, str)
				for con := range js.Consumers(str.Config.Name) {
					if ctx.Err() != nil {
						return
					}
					o.ConsumerHandler(con)
				}
			}
		case <-ctx.Done():
			o.logger.Debugln("stopping JetStream config poll loop")
			return
		}
	}
}

func (o *jsConfigListListener) Start(natsCtx *NatsContext) error {
	o.Lock()
	defer o.Unlock()
	if o.conn != nil {
		// already started
		return nil
	}
	conn, err := o.provider.Get(natsCtx)
	if err != nil {
		return fmt.Errorf("nats connection failed. error: %v", err)
	}
	o.conn = conn
	js, err := conn.Conn().JetStream()
	if err != nil {
		o.conn.Close()
		o.conn = nil
		return fmt.Errorf("failed to create jetstream connection: %v", err)
	}
	o.js = js
	ctx, cancelFunc := context.WithCancel(context.Background())

	o.cancelLoop = cancelFunc
	o.wg.Add(1)
	go o.gatherData(ctx, o.opts.ScrapeInterval)
	return nil
}

func (o *jsConfigListListener) StreamHandler(streamInfo *nats.StreamInfo) {
	if streamInfo == nil {
		o.logger.Infof("received empty stream")
		return
	}
	o.metrics.jsStreamConfig.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})

	o.metrics.jsStreamConfig.With(
		prometheus.Labels{
			"discard_policy": streamInfo.Config.Discard.String(),
			"storage_type":   streamInfo.Config.Storage.String(),
			"replica_number": strconv.Itoa(streamInfo.Config.Replicas),
			"stream_name":    streamInfo.Config.Name,
		},
	).Set(1)

	o.metrics.jsStreamRaftInfo.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})
	o.metrics.jsStreamRaftPeerInfo.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})
	o.metrics.jsStreamReplicationLag.DeletePartialMatch(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	)

	if streamInfo.Cluster != nil {
		o.metrics.jsStreamRaftInfo.With(
			prometheus.Labels{
				"stream_name":   streamInfo.Config.Name,
				"leader":        streamInfo.Cluster.Leader,
				"replica_count": strconv.Itoa(len(streamInfo.Cluster.Replicas)),
			},
		).Set(1)

		for _, peer := range streamInfo.Cluster.Replicas {
			o.metrics.jsStreamRaftPeerInfo.With(
				prometheus.Labels{
					"leader":      streamInfo.Cluster.Leader,
					"stream_name": streamInfo.Config.Name,
					"peer_name":   peer.Name,
					"offline":     convertBoolToString(peer.Offline),
					"current":     convertBoolToString(peer.Current),
				},
			).Set(1)

			o.metrics.jsStreamReplicationLag.With(
				prometheus.Labels{
					"stream_name": streamInfo.Config.Name,
					"peer_name":   peer.Name,
				},
			).Set(float64(peer.Lag))
		}
	}

	o.metrics.jsStreamStateConsumerNum.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})
	o.metrics.jsStreamStateSubjectNum.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})
	o.metrics.jsStreamStateDeletedNum.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})

	o.metrics.jsStreamStateSubjectNum.With(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	).Set(float64(streamInfo.State.NumSubjects))
	o.metrics.jsStreamStateDeletedNum.With(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	).Set(float64(streamInfo.State.NumDeleted))
	o.metrics.jsStreamStateConsumerNum.With(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	).Set(float64(streamInfo.State.Consumers))

	// Sequence numbers come free with the STREAM.LIST response the poll loop
	// already makes, so these cost no extra request.
	o.metrics.jsStreamStateFirstSeq.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})
	o.metrics.jsStreamStateLastSeq.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})

	o.metrics.jsStreamStateFirstSeq.With(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	).Set(float64(streamInfo.State.FirstSeq))
	o.metrics.jsStreamStateLastSeq.With(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	).Set(float64(streamInfo.State.LastSeq))

	// Stream Limits
	o.metrics.jsStreamLimitMaxMsgs.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})
	o.metrics.jsStreamLimitMaxMsgsPer.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})
	o.metrics.jsStreamLimitMaxBytes.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})
	o.metrics.jsStreamLimitMaxAge.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})
	o.metrics.jsStreamLimitMaxMsgSize.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})
	o.metrics.jsStreamLimitMaxConsumers.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})

	o.metrics.jsStreamLimitMaxMsgs.With(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	).Set(float64(streamInfo.Config.MaxMsgs))

	o.metrics.jsStreamLimitMaxMsgsPer.With(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	).Set(float64(streamInfo.Config.MaxMsgsPerSubject))

	o.metrics.jsStreamLimitMaxBytes.With(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	).Set(float64(streamInfo.Config.MaxBytes))

	o.metrics.jsStreamLimitMaxAge.With(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	).Set(streamInfo.Config.MaxAge.Seconds())

	o.metrics.jsStreamLimitMaxMsgSize.With(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	).Set(float64(streamInfo.Config.MaxMsgSize))

	o.metrics.jsStreamLimitMaxConsumers.With(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	).Set(float64(streamInfo.Config.MaxConsumers))
}

// SubjectsHandler exposes the stream's per-subject message counts. This needs a
// second STREAM.INFO request carrying a subjects filter — STREAM.LIST does not
// return per-subject state — so it only runs when explicitly enabled, and only
// for streams whose subject count stays under the configured cap.
func (o *jsConfigListListener) SubjectsHandler(js nats.JetStreamContext, streamInfo *nats.StreamInfo) {
	if !o.opts.Subjects || streamInfo == nil || js == nil {
		return
	}

	name := streamInfo.Config.Name
	if o.subjectStreams != nil {
		if _, ok := o.subjectStreams[name]; !ok {
			return
		}
	}

	streamLabels := prometheus.Labels{"stream_name": name}
	o.metrics.jsStreamSubjectMsgs.DeletePartialMatch(streamLabels)
	o.metrics.jsStreamSubjectsSkipped.DeletePartialMatch(streamLabels)

	// NumSubjects comes from the STREAM.LIST response, so this cap is applied
	// before paying for the per-subject request rather than after.
	if streamInfo.State.NumSubjects > uint64(o.opts.SubjectsMax) {
		o.logger.Warnf(
			"skipping per-subject collection for stream %q: %d subjects exceeds the cap of %d",
			name, streamInfo.State.NumSubjects, o.opts.SubjectsMax,
		)
		o.metrics.jsStreamSubjectsSkipped.With(streamLabels).Set(1)
		return
	}
	o.metrics.jsStreamSubjectsSkipped.With(streamLabels).Set(0)

	info, err := js.StreamInfo(name, &nats.StreamInfoRequest{SubjectsFilter: ">"})
	if err != nil {
		o.logger.Warnf("failed to fetch per-subject state for stream %q: %v", name, err)
		return
	}
	if info == nil {
		return
	}

	for subject, msgs := range info.State.Subjects {
		o.metrics.jsStreamSubjectMsgs.With(
			prometheus.Labels{
				"stream_name": name,
				"subject":     subject,
			},
		).Set(float64(msgs))
	}
}

func convertBoolToString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
func (o *jsConfigListListener) ConsumerHandler(consumerInfo *nats.ConsumerInfo) {
	if consumerInfo == nil {
		o.logger.Infof("received empty consumer")
		return
	}
	consumerLabels := prometheus.Labels{
		"stream_name":   consumerInfo.Stream,
		"consumer_name": consumerInfo.Name,
	}

	o.metrics.jsConsumerConfig.DeletePartialMatch(consumerLabels)
	o.metrics.jsConsumerConfig.With(
		prometheus.Labels{
			"stream_name":     consumerInfo.Stream,
			"consumer_name":   consumerInfo.Name,
			"max_pending_ack": strconv.Itoa(consumerInfo.Config.MaxAckPending),
			"ack_policy":      consumerInfo.Config.AckPolicy.String(),
			"is_pull":         IsPullBased(consumerInfo),
		},
	).Set(1)

	o.metrics.jsConsumerState.DeletePartialMatch(consumerLabels)
	o.metrics.jsConsumerState.With(consumerLabels).Set(1)

	o.metrics.jsConsumerWaitingNum.DeletePartialMatch(consumerLabels)
	o.metrics.jsConsumerPendingNum.DeletePartialMatch(consumerLabels)
	o.metrics.jsConsumerAckPendingNum.DeletePartialMatch(consumerLabels)
	o.metrics.jsConsumerRedeliveredNum.DeletePartialMatch(consumerLabels)
	o.metrics.jsConsumerLastDeliveredConsumer.DeletePartialMatch(consumerLabels)
	o.metrics.jsConsumerLastDeliveredStream.DeletePartialMatch(consumerLabels)
	o.metrics.jsConsumerLastAckFloorConsumer.DeletePartialMatch(consumerLabels)
	o.metrics.jsConsumerLastAckFloorStream.DeletePartialMatch(consumerLabels)

	o.metrics.jsConsumerWaitingNum.With(consumerLabels).Set(float64(consumerInfo.NumWaiting))
	o.metrics.jsConsumerPendingNum.With(consumerLabels).Set(float64(consumerInfo.NumPending))
	o.metrics.jsConsumerAckPendingNum.With(consumerLabels).Set(float64(consumerInfo.NumAckPending))
	o.metrics.jsConsumerRedeliveredNum.With(consumerLabels).Set(float64(consumerInfo.NumRedelivered))
	o.metrics.jsConsumerLastDeliveredConsumer.With(consumerLabels).Set(float64(consumerInfo.Delivered.Consumer))
	o.metrics.jsConsumerLastDeliveredStream.With(consumerLabels).Set(float64(consumerInfo.Delivered.Stream))
	o.metrics.jsConsumerLastAckFloorConsumer.With(consumerLabels).Set(float64(consumerInfo.AckFloor.Consumer))
	o.metrics.jsConsumerLastAckFloorStream.With(consumerLabels).Set(float64(consumerInfo.AckFloor.Stream))

	o.metrics.jsConsumerRaftInfo.DeletePartialMatch(consumerLabels)
	o.metrics.jsConsumerRaftPeerInfo.DeletePartialMatch(consumerLabels)
	o.metrics.jsConsumerReplicationLag.DeletePartialMatch(consumerLabels)

	if consumerInfo.Cluster == nil {
		// non-clustered (single-server or R1) deployment — no raft/peer data
		return
	}
	o.metrics.jsConsumerRaftInfo.With(
		prometheus.Labels{
			"consumer_name": consumerInfo.Name,
			"stream_name":   consumerInfo.Stream,
			"leader":        consumerInfo.Cluster.Leader,
			"replica_count": strconv.Itoa(len(consumerInfo.Cluster.Replicas)),
		},
	).Set(1)
	for _, peer := range consumerInfo.Cluster.Replicas {
		o.metrics.jsConsumerRaftPeerInfo.With(
			prometheus.Labels{
				"leader":        consumerInfo.Cluster.Leader,
				"stream_name":   consumerInfo.Stream,
				"consumer_name": consumerInfo.Name,
				"peer_name":     peer.Name,
				"offline":       convertBoolToString(peer.Offline),
				"current":       convertBoolToString(peer.Current),
			},
		).Set(1)
		o.metrics.jsConsumerReplicationLag.With(
			prometheus.Labels{
				"stream_name":   consumerInfo.Stream,
				"consumer_name": consumerInfo.Name,
				"peer_name":     peer.Name,
			},
		).Set(float64(peer.Lag))
	}
}
func IsPullBased(info *nats.ConsumerInfo) string {
	isPull := info.Config.DeliverGroup == "" && info.Config.DeliverSubject == ""
	if isPull {
		return "true"
	}
	return "false"
}

// Stop stops the JetStream config polling loop.
func (o *jsConfigListListener) Stop() {
	o.Lock()
	if o.conn == nil {
		// already stopped
		o.Unlock()
		return
	}
	cancel := o.cancelLoop
	o.Unlock()

	cancel()
	o.wg.Wait()

	o.Lock()
	if o.conn != nil {
		o.conn.Close()
		o.conn = nil
		o.js = nil
	}
	o.Unlock()
}
