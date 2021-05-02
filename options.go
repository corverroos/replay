package replay

import "time"

type options struct {
	shardM, shardN  int
	shardFunc       func(n int, run string) (m int)
	nameFunc        func(fn interface{}) string
	workflowMetrics func(namespace, workflow string) Metrics
	activityMetrics func(namespace, activity string) Metrics
	awaitTimeout    time.Duration
}

type option func(*options)

// WithName returns an option to explicitly define a workflow or activity name.
// Default behavior infers function names via reflection.
func WithName(name string) option {
	return func(o *options) {
		o.nameFunc = func(_ interface{}) string {
			return name
		}
	}
}

// WithShard returns an option to only consume a subset of all runs (a shard).
// The subset is defined by runs consistently hashing onto the mth of n shards.
//
// Note that all m-of-n shards must be registered independently and that n should
// not be changed since the cursors are tied to it. However since replay is
// deterministic and idempotent, resetting the consumer cursors by changing n
// should only result in an initial delay (and load spike) as the newly sharded
// consumers reprocess all runs and activities.
//
// Note that each activity and each workflow can be sharded separately.
//
// Sharding is disabled by default, which is similar to 1-of-1 sharding.
// See also WithShardFunc.
func WithShard(m, n int) option {
	return func(o *options) {
		o.shardM = m
		o.shardN = n
	}
}

// WithShardFunc returns an option to override the default shard function
// which uses Google's Jump Consistent Hash with the fnv64a hash of the run as its key.
//
// Note that sharding needs to be enabled via WithShard.
// Note also that all m-of-n shards need to use the same deterministic shard function.
func WithShardFunc(fn func(n int, run string) (m int)) option {
	return func(o *options) {
		o.shardFunc = fn
	}
}

// WithAwaitTimeout returns an option to override the default run goroutine
// await activity response timeout of 1 hour.
//
// This timeout ensures there isn't an buildup of run goroutines
// for long sleeps.
//
// This function only applies to RegisterWorkflow.
func WithAwaitTimeout(d time.Duration) option {
	return func(o *options) {
		o.awaitTimeout = d
	}
}

// WithWorkflowMetrics returns an option to define workflow metrics.
// It overrides the default prometheus metrics.
//
// This function only applies to RegisterWorkflow.
func WithWorkflowMetrics(m func(namespace, workflow string) Metrics) option {
	return func(o *options) {
		o.workflowMetrics = m
	}
}

// WithActivityMetrics returns an option to define activity metrics.
// It overrides the default prometheus metrics.
// This function only applies to RegisterActivity and IncStart is not used.
func WithActivityMetrics(m func(namespace, activity string) Metrics) option {
	return func(o *options) {
		o.activityMetrics = m
	}
}

type Metrics struct {
	IncErrors   func()
	IncStart    func()
	IncComplete func(time.Duration)
}

func defaultOptions() options {
	return options{
		shardFunc:    defaultShardFunc,
		nameFunc:     getFunctionName,
		awaitTimeout: time.Hour,
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
