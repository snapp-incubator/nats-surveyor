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
	streamRaftPeerInfoLabels     = []string{"stream_name", "peer_name", "offline", "current", "leader", "lag"}
	consumerRaftInfoLabels       = []string{"consumer_name", "leader", "replica_count", "stream_name"}
	consumerStateLabels          = []string{"consumer_name", "stream_name", "last_delivered_message_consumer", "last_delivered_message_stream", "ack_floor_consumer", "ack_floor_stream"}
	consumerRaftPeerInfoLabels   = []string{"stream_name", "consumer_name", "peer_name", "offline", "current", "leader", "lag"}
	streamReplicationLagLabels   = []string{"stream_name", "peer_name"}
	consumerReplicationLagLabels = []string{"stream_name", "consumer_name", "peer_name"}
	DefaultScrapeInterval        = 10 * time.Second
	streamLabel                  = []string{"stream_name"}
	consumerLabel                = []string{"consumer_name"}
	consumerStreamLabel          = []string{"consumer_name", "stream_name"}

	//DefaultListenerID     = "default_listener"
)

type JSStreamConfigMetrics struct {
	jsStreamConfig           *prometheus.GaugeVec
	jsStreamRaftInfo         *prometheus.GaugeVec
	jsStreamRaftPeerInfo     *prometheus.GaugeVec
	jsStreamReplicationLag   *prometheus.GaugeVec
	jsStreamStateConsumerNum *prometheus.GaugeVec
	jsStreamStateDeletedNum  *prometheus.GaugeVec
	jsStreamStateSubjectNum  *prometheus.GaugeVec

	jsConsumerConfig              *prometheus.GaugeVec
	jsConsumerState               *prometheus.GaugeVec
	jsConsumerRaftInfo            *prometheus.GaugeVec
	jsConsumerRaftPeerInfo        *prometheus.GaugeVec
	jsConsumerReplicationLag      *prometheus.GaugeVec
	jsConsumerAckPendingNum       *prometheus.GaugeVec
	jsConsumerPendingNum          *prometheus.GaugeVec
	jsConsumerRedeliveredNum      *prometheus.GaugeVec
	jsConsumerWaitingNum          *prometheus.GaugeVec
	jsConsumerLastAckFloorSecNum  *prometheus.GaugeVec
	jsConsumerLastDeliveredSecNum *prometheus.GaugeVec

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
		jsConsumerLastAckFloorSecNum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_last_ack_floor_number"),
			Help:        "last ack floor number of consumer",
			ConstLabels: constLabels,
		}, consumerStreamLabel),
		jsConsumerLastDeliveredSecNum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        prometheus.BuildFQName("nats", "jetstream", "consumer_last_delivered_number"),
			Help:        "last delivered number of consumer",
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

	registry.MustRegister(metrics.jsConsumerRaftInfo)
	registry.MustRegister(metrics.jsConsumerRaftPeerInfo)
	registry.MustRegister(metrics.jsStreamReplicationLag)
	registry.MustRegister(metrics.jsConsumerReplicationLag)
	registry.MustRegister(metrics.jsConsumerWaitingNum)
	registry.MustRegister(metrics.jsConsumerAckPendingNum)
	registry.MustRegister(metrics.jsConsumerPendingNum)
	registry.MustRegister(metrics.jsConsumerRedeliveredNum)
	registry.MustRegister(metrics.jsConsumerLastDeliveredSecNum)
	registry.MustRegister(metrics.jsConsumerLastAckFloorSecNum)

	registry.MustRegister(metrics.jsStreamLimitMaxMsgs)
	registry.MustRegister(metrics.jsStreamLimitMaxMsgsPer)
	registry.MustRegister(metrics.jsStreamLimitMaxBytes)
	registry.MustRegister(metrics.jsStreamLimitMaxAge)
	registry.MustRegister(metrics.jsStreamLimitMaxMsgSize)
	registry.MustRegister(metrics.jsStreamLimitMaxConsumers)

	return metrics
}

// jsAdvisoryListener listens for JetStream advisories and expose them as prometheus data
type jsConfigListListener struct {
	sync.Mutex
	cancelLoop context.CancelFunc
	cp         *natsConnPool
	logger     *logrus.Logger
	metrics    *JSStreamConfigMetrics
	pc         *pooledNatsConn
	js         nats.JetStreamContext
}

func NewJetStreamConfigListener(cp *natsConnPool, logger *logrus.Logger, metrics *JSStreamConfigMetrics) *jsConfigListListener {

	return &jsConfigListListener{
		cp:      cp,
		logger:  logger,
		metrics: metrics,
	}
}

func (o *jsConfigListListener) gatherData(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	o.logger.Infoln("starting config list listener ticker")
	for {
		select {
		case <-ticker.C:
			o.Lock()
			for str := range o.js.Streams() {
				o.StreamHandler(str)
				for con := range o.js.Consumers(str.Config.Name) {
					o.ConsumerHandler(con)
				}
			}
			o.Unlock()
		// do operation
		case <-ctx.Done():
			fmt.Println("shutting down data gathering")
			return
		}
	}
}

func (o *jsConfigListListener) Start(natsCtx *natsContext) error {
	o.Lock()
	defer o.Unlock()
	if o.pc != nil {
		// already started
		return nil
	}
	pc, err := o.cp.Get(natsCtx)
	if err != nil {
		return fmt.Errorf("nats connection failed. error: %v", err)
	}
	o.pc = pc
	js, err := pc.nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to create jetstream connection")
	}
	o.js = js
	ctx, cancelFunc := context.WithCancel(context.Background())

	o.cancelLoop = cancelFunc
	go o.gatherData(ctx, DefaultScrapeInterval)
	o.logger.Infof("started JetStream Stream List for metric topic")
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
	o.metrics.jsStreamRaftInfo.With(
		prometheus.Labels{
			"stream_name":   streamInfo.Config.Name,
			"leader":        streamInfo.Cluster.Leader,
			"replica_count": strconv.Itoa(len(streamInfo.Cluster.Replicas)),
		},
	).Set(1)

	o.metrics.jsStreamRaftPeerInfo.DeletePartialMatch(prometheus.Labels{
		"stream_name": streamInfo.Config.Name,
	})
	o.metrics.jsStreamReplicationLag.DeletePartialMatch(
		prometheus.Labels{
			"stream_name": streamInfo.Config.Name,
		},
	)

	for _, peer := range streamInfo.Cluster.Replicas {
		o.metrics.jsStreamRaftPeerInfo.With(
			prometheus.Labels{
				"leader":      streamInfo.Cluster.Leader,
				"stream_name": streamInfo.Config.Name,
				"peer_name":   peer.Name,
				"offline":     convertBoolToString(peer.Offline),
				"current":     convertBoolToString(peer.Current),
				"lag":         strconv.FormatUint(peer.Lag, 10),
			},
		).Set(1)

		o.metrics.jsStreamReplicationLag.With(
			prometheus.Labels{
				"stream_name": streamInfo.Config.Name,
				"peer_name":   peer.Name,
			},
		).Set(float64(peer.Lag))
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
	o.metrics.jsConsumerConfig.DeletePartialMatch(prometheus.Labels{
		"stream_name":   consumerInfo.Stream,
		"consumer_name": consumerInfo.Name,
	})
	o.metrics.jsConsumerConfig.With(
		prometheus.Labels{
			"stream_name":     consumerInfo.Stream,
			"consumer_name":   consumerInfo.Name,
			"max_pending_ack": strconv.Itoa(consumerInfo.Config.MaxAckPending),
			"ack_policy":      consumerInfo.Config.AckPolicy.String(),
			"is_pull":         IsPullBased(consumerInfo),
		},
	).Set(1)
	o.metrics.jsConsumerState.DeletePartialMatch(prometheus.Labels{
		"stream_name":   consumerInfo.Stream,
		"consumer_name": consumerInfo.Name,
	})
	o.metrics.jsConsumerState.With(
		prometheus.Labels{
			"consumer_name":                   consumerInfo.Name,
			"stream_name":                     consumerInfo.Stream,
			"last_delivered_message_consumer": strconv.FormatUint(consumerInfo.Delivered.Consumer, 10),
			"last_delivered_message_stream":   strconv.FormatUint(consumerInfo.Delivered.Stream, 10),
			"ack_floor_consumer":              strconv.FormatUint(consumerInfo.AckFloor.Consumer, 10),
			"ack_floor_stream":                strconv.FormatUint(consumerInfo.AckFloor.Stream, 10),
		}).Set(1)

	o.metrics.jsConsumerRaftInfo.DeletePartialMatch(prometheus.Labels{
		"stream_name":   consumerInfo.Stream,
		"consumer_name": consumerInfo.Name,
	})
	o.metrics.jsConsumerRaftInfo.With(
		prometheus.Labels{
			"consumer_name": consumerInfo.Name,
			"stream_name":   consumerInfo.Stream,
			"leader":        consumerInfo.Cluster.Leader,
			"replica_count": strconv.Itoa(len(consumerInfo.Cluster.Replicas)),
		},
	).Set(1)

	o.metrics.jsConsumerRaftPeerInfo.DeletePartialMatch(prometheus.Labels{
		"stream_name":   consumerInfo.Stream,
		"consumer_name": consumerInfo.Name,
	})
	o.metrics.jsConsumerReplicationLag.DeletePartialMatch(
		prometheus.Labels{
			"stream_name":   consumerInfo.Stream,
			"consumer_name": consumerInfo.Name,
		},
	)
	for _, peer := range consumerInfo.Cluster.Replicas {
		o.metrics.jsConsumerRaftPeerInfo.With(
			prometheus.Labels{
				"leader":        consumerInfo.Cluster.Leader,
				"stream_name":   consumerInfo.Stream,
				"consumer_name": consumerInfo.Name,
				"peer_name":     peer.Name,
				"offline":       convertBoolToString(peer.Offline),
				"current":       convertBoolToString(peer.Current),
				"lag":           strconv.FormatUint(peer.Lag, 10),
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
	o.metrics.jsConsumerWaitingNum.DeletePartialMatch(prometheus.Labels{
		"consumer_name": consumerInfo.Config.Name,
		"stream_name":   consumerInfo.Stream,
	})
	o.metrics.jsConsumerPendingNum.DeletePartialMatch(prometheus.Labels{
		"consumer_name": consumerInfo.Config.Name,
		"stream_name":   consumerInfo.Stream,
	})
	o.metrics.jsConsumerAckPendingNum.DeletePartialMatch(prometheus.Labels{
		"consumer_name": consumerInfo.Config.Name,
		"stream_name":   consumerInfo.Stream,
	})
	o.metrics.jsConsumerRedeliveredNum.DeletePartialMatch(prometheus.Labels{
		"consumer_name": consumerInfo.Config.Name,
		"stream_name":   consumerInfo.Stream,
	})

	o.metrics.jsConsumerWaitingNum.With(
		prometheus.Labels{
			"consumer_name": consumerInfo.Config.Name,
			"stream_name":   consumerInfo.Stream,
		},
	).Set(float64(consumerInfo.NumWaiting))
	o.metrics.jsConsumerPendingNum.With(
		prometheus.Labels{
			"consumer_name": consumerInfo.Config.Name,
			"stream_name":   consumerInfo.Stream,
		},
	).Set(float64(consumerInfo.NumPending))
	o.metrics.jsConsumerAckPendingNum.With(
		prometheus.Labels{
			"consumer_name": consumerInfo.Config.Name,
			"stream_name":   consumerInfo.Stream,
		},
	).Set(float64(consumerInfo.NumAckPending))
	o.metrics.jsConsumerRedeliveredNum.With(
		prometheus.Labels{
			"consumer_name": consumerInfo.Config.Name,
			"stream_name":   consumerInfo.Stream,
		},
	).Set(float64(consumerInfo.NumRedelivered))

	o.metrics.jsConsumerLastDeliveredSecNum.DeletePartialMatch(prometheus.Labels{
		"consumer_name": consumerInfo.Config.Name,
		"stream_name":   consumerInfo.Stream,
	})
	o.metrics.jsConsumerLastAckFloorSecNum.DeletePartialMatch(prometheus.Labels{
		"consumer_name": consumerInfo.Config.Name,
		"stream_name":   consumerInfo.Stream,
	})

	o.metrics.jsConsumerLastDeliveredSecNum.With(
		prometheus.Labels{
			"consumer_name": consumerInfo.Config.Name,
			"stream_name":   consumerInfo.Stream,
		},
	).Set(float64(consumerInfo.Delivered.Consumer))
	o.metrics.jsConsumerLastAckFloorSecNum.With(
		prometheus.Labels{
			"consumer_name": consumerInfo.Config.Name,
			"stream_name":   consumerInfo.Stream,
		},
	).Set(float64(consumerInfo.AckFloor.Consumer))
}
func IsPullBased(info *nats.ConsumerInfo) string {
	isPull := info.Config.DeliverGroup == "" && info.Config.DeliverSubject == ""
	if isPull {
		return "true"
	}
	return "false"
}

// Stop stops listening for JetStream advisories
func (o *jsConfigListListener) Stop() {
	o.Lock()
	defer o.Unlock()
	if o.pc == nil {
		// already stopped
		return
	}
	o.cancelLoop()
	o.pc.ReturnToPool()
	o.pc = nil
}
