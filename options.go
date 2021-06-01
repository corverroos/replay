package replay

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/dgryski/go-jump"
	"github.com/luno/reflex"
)

type options struct {
	shardName       string
	shardFunc       func(run string) bool
	nameFunc        func(fn interface{}) string
	workflowMetrics func(namespace, workflow string) Metrics
	activityMetrics func(namespace, activity string) Metrics
	awaitTimeout    time.Duration
	consumerOpts    []reflex.ConsumerOption
}

// Option defines a functional option to configure workflow and activity consumers.
type Option func(*options)

// WithName returns an option to explicitly define a workflow or activity name.
// Default behavior infers function names via reflection.
func WithName(name string) Option {
	return func(o *options) {
		o.nameFunc = func(_ interface{}) string {
			return name
		}
	}
}

// WithShard returns an option for a workflow or activity consumer to only consume
// a subset of all runs (a shard). The subset is defined by the shard functioning
// returning true. The shard name is added to the reflex cursor name to uniquely
// identity each shard.
//
// Note that all shards must be registered independently and that the shard func
// should be deterministic and may never change. However since replay is
// deterministic and idempotent, resharding the consumer by changing the
// shard func and the name should only result in an initial delay
// (and load spike) as the newly sharded consumers reprocess all runs and activities.
//
// Note that each activity and each workflow can be sharded separately.
//
// Sharding is disabled by default, which is similar to a 1-of-1 WithHashedShard.
func WithShard(name string, fn func(run string) bool) Option {
	return func(o *options) {
		o.shardName = name
		o.shardFunc = fn
	}
}

// WithHashedShard returns an option to configure a m-on-n consistent hashing shard
// using Google's Jump Consistent with the fnv64a hash of the run as its key.
// See WithShard for more detail.
func WithHashedShard(m, n int) Option {
	return WithShard(
		fmt.Sprintf("%d_%d", m+1, n),
		func(run string) bool {
			h := fnv.New64a()
			h.Write([]byte(run))
			return int(jump.Hash(h.Sum64(), n)) == m
		})
}

// WithAwaitTimeout returns an option to override the default run goroutine
// await activity response timeout of 1 hour.
//
// This timeout ensures there isn't an buildup of run goroutines
// for long sleeps.
//
// This function only applies to RegisterWorkflow.
func WithAwaitTimeout(d time.Duration) Option {
	return func(o *options) {
		o.awaitTimeout = d
	}
}

// WithWorkflowMetrics returns an option to define workflow metrics.
// It overrides the default prometheus metrics.
//
// This function only applies to RegisterWorkflow.
func WithWorkflowMetrics(m func(namespace, workflow string) Metrics) Option {
	return func(o *options) {
		o.workflowMetrics = m
	}
}

// WithActivityMetrics returns an option to define activity metrics.
// It overrides the default prometheus metrics.
// This function only applies to RegisterActivity and IncStart is not used.
func WithActivityMetrics(m func(namespace, activity string) Metrics) Option {
	return func(o *options) {
		o.activityMetrics = m
	}
}

// WithConsumerOpts returns an option to define custom reflex consumer options.
// It overrides the default options that specify reflex.WithoutConsumerActivityTTL.
// This function only applies to RegisterActivity and RegisterWorkflow.
func WithConsumerOpts(opts ...reflex.ConsumerOption) Option {
	return func(o *options) {
		o.consumerOpts = opts
	}
}

type Metrics struct {
	IncErrors   func()
	IncStart    func()
	IncComplete func(time.Duration)
}

func defaultOptions() options {
	return options{
		shardFunc:    func(run string) bool { return true },
		nameFunc:     getFunctionName,
		awaitTimeout: time.Hour,
		consumerOpts: []reflex.ConsumerOption{reflex.WithoutConsumerActivityTTL()},
		workflowMetrics: func(namespace, workflow string) Metrics {
			return Metrics{
				IncErrors: workflowErrors.WithLabelValues(namespace, workflow).Inc,
				IncStart:  runStarted.WithLabelValues(namespace, workflow).Inc,
				IncComplete: func(d time.Duration) {
					runCompleted.WithLabelValues(namespace, workflow).Observe(d.Seconds())
				},
			}
		},
		activityMetrics: func(namespace, activity string) Metrics {
			return Metrics{
				IncErrors: activityErrors.WithLabelValues(namespace, activity).Inc,
				IncComplete: func(d time.Duration) {
					activityDuration.WithLabelValues(namespace, activity).Observe(d.Seconds())
				},
			}
		},
	}
}
