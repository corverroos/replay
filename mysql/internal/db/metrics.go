package db

import "github.com/prometheus/client_golang/prometheus"

var eventsGapFilledCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "replay",
	Subsystem: "events_table",
	Name:      "gap_filled_total",
	Help:      "Total number of gaps filled",
})

func init() {
	prometheus.MustRegister(eventsGapFilledCounter)
}
