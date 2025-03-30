package memory

import (
	"sync/atomic"
	"time"
)

// metrics tracks operational metrics for the worker
type metrics struct {
	tasksCompleted uint64
	tasksFailed    uint64
	processingTime *histogram
}

// newMetrics creates a new metrics instance
func newMetrics() *metrics {
	return &metrics{
		processingTime: newHistogram(),
	}
}

// histogram is a simple histogram implementation
// In a production system, you'd use a proper metrics library
type histogram struct {
	count uint64
	sum   int64
	min   int64
	max   int64
}

// newHistogram creates a new histogram
func newHistogram() *histogram {
	return &histogram{
		min: int64(^uint64(0) >> 1), // Max int64
		max: 0,
	}
}

// Record adds a value to the histogram
func (h *histogram) Record(duration time.Duration) {
	durationMs := duration.Milliseconds()

	atomic.AddUint64(&h.count, 1)
	atomic.AddInt64(&h.sum, durationMs)

	// Update min (using CAS for thread safety)
	for {
		currentMin := atomic.LoadInt64(&h.min)
		if durationMs >= currentMin {
			break
		}
		if atomic.CompareAndSwapInt64(&h.min, currentMin, durationMs) {
			break
		}
	}

	// Update max (using CAS for thread safety)
	for {
		currentMax := atomic.LoadInt64(&h.max)
		if durationMs <= currentMax {
			break
		}
		if atomic.CompareAndSwapInt64(&h.max, currentMax, durationMs) {
			break
		}
	}
}

// Stats returns the current histogram statistics
func (h *histogram) Stats() map[string]int64 {
	count := atomic.LoadUint64(&h.count)
	sum := atomic.LoadInt64(&h.sum)
	min := atomic.LoadInt64(&h.min)
	max := atomic.LoadInt64(&h.max)

	var avg int64
	if count > 0 {
		avg = sum / int64(count)
	}

	// If no data has been recorded, set min to 0
	if min == int64(^uint64(0)>>1) {
		min = 0
	}

	return map[string]int64{
		"count": int64(count),
		"sum":   sum,
		"min":   min,
		"max":   max,
		"avg":   avg,
	}
}
