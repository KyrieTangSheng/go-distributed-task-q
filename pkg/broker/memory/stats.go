package memory

import (
	"context"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
)

// Stats returns current queue statistics
func (b *MemoryBroker) Stats(ctx context.Context) (broker.Stats, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := broker.Stats{
		Pending:   0,
		Running:   0,
		Completed: 0,
		Failed:    0,
		Retrying:  0,
		Total:     len(b.tasks),
		UpdatedAt: time.Now().UTC(),
	}

	// Count tasks by state
	for _, t := range b.tasks {
		switch t.GetState() {
		case task.StatePending:
			stats.Pending++
		case task.StateRunning:
			stats.Running++
		case task.StateCompleted:
			stats.Completed++
		case task.StateFailed:
			stats.Failed++
		case task.StateRetryable:
			stats.Retrying++
		}
	}

	return stats, nil
}
