package replay

import "github.com/prometheus/client_golang/prometheus"

var (
	workflowErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "replay",
		Subsystem: "client",
		Name:      "workflow_consumer_errors_total",
		Help:      "Total number of errors returned by a workflow consumer.",
	}, []string{"workflow"})

	runStarted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "replay",
		Subsystem: "client",
		Name:      "workflow_run_started_total",
		Help:      "Total number of workflow runs started.",
	}, []string{"workflow"})

	runCompleted = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "replay",
		Subsystem: "client",
		Name:      "workflow_run_completed_duration_seconds",
		Help:      "Workflow runs start to complete duration in seconds.",
		Buckets:   []float64{1, 60, 10 * 60, 60 * 60, 24 * 60 * 60, 7 * 24 * 60 * 60, 30 * 24 * 60 * 60, 356 * 24 * 60 * 60},
	}, []string{"workflow"})
)

func init() {
	prometheus.MustRegister(
		workflowErrors,
		runStarted,
		runCompleted,
	)
}
