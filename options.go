package replay

import "time"

type options struct {
	nameFunc        func(interface{}) string
	workflowMetrics func(string) Metrics
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
func WithWorkflowMetrics(m func(workflow string) Metrics) option {
	return func(o *options) {
		o.workflowMetrics = m
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
		workflowMetrics: func(workflow string) Metrics {
			return Metrics{
				IncErrors: workflowErrors.WithLabelValues(workflow).Inc,
				IncStart:  runStarted.WithLabelValues(workflow).Inc,
				IncComplete: func(d time.Duration) {
					runCompleted.WithLabelValues(workflow).Observe(d.Seconds())
				},
			}
		},
	}
}
