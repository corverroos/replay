package replay

import "time"

type options struct {
	nameFunc        func(fn interface{}) string
	workflowMetrics func(namespace, workflow string) Metrics
	activityMetrics func(namespace, activity string) Metrics
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

// WithWorkflowMetrics returns an option to define workflow metrics.
// It overrides the default prometheus metrics.
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
		nameFunc: getFunctionName,
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
