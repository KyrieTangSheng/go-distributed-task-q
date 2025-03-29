package memory

import (
	"context"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"go.uber.org/zap"
)

// SubmitTask adds a new task to the queue
func (b *MemoryBroker) SubmitTask(ctx context.Context, t *task.Task) error {
	// Check if broker is shutting down
	if b.isShuttingDown() {
		return broker.ErrBrokerClosed
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Check for duplicate idempotency key
	idempotencyKey := t.GetIdempotencyKey()
	if idempotencyKey != "" {
		if existingID, found := b.idempotencyMap[idempotencyKey]; found {
			b.logger.Info("Duplicate task submission",
				zap.String("idempotency_key", idempotencyKey),
				zap.String("existing_task_id", existingID))
			return broker.ErrDuplicateTask
		}
	}

	// Clone the task to avoid external modifications
	taskCopy := t.Clone()
	taskID := taskCopy.GetID()

	// Store the task
	b.tasks[taskID] = taskCopy

	// Map idempotency key if present
	if idempotencyKey != "" {
		b.idempotencyMap[idempotencyKey] = taskID
	}

	// Add to queue based on state
	switch taskCopy.GetState() {
	case task.StatePending:
		// Add to pending queue with priority
		b.pendingQueue.PushTask(taskID, taskCopy.GetPriority(), time.Now())

		// Update metrics
		b.metrics.QueueDepth = int64(b.pendingQueue.Len())
		b.metrics.TasksSubmitted++

		b.logger.Info("Task submitted",
			zap.String("task_id", taskID),
			zap.String("task_type", taskCopy.GetType()),
			zap.Int("priority", taskCopy.GetPriority()),
			zap.Int("queue_depth", b.pendingQueue.Len()))

	case task.StateScheduled:
		// For scheduled tasks, use the scheduled time
		if nextRun := taskCopy.GetNextRunTime(); nextRun != nil {
			b.pendingQueue.PushTask(taskID, taskCopy.GetPriority(), *nextRun)

			// Update metrics
			b.metrics.QueueDepth = int64(b.pendingQueue.Len())
			b.metrics.TasksSubmitted++

			b.logger.Info("Scheduled task submitted",
				zap.String("task_id", taskID),
				zap.String("task_type", taskCopy.GetType()),
				zap.Time("scheduled_time", *nextRun),
				zap.Int("queue_depth", b.pendingQueue.Len()))
		} else {
			// If no scheduled time, treat as pending
			b.pendingQueue.PushTask(taskID, taskCopy.GetPriority(), time.Now())

			b.logger.Warn("Scheduled task missing execution time, treating as pending",
				zap.String("task_id", taskID))
		}

	default:
		// For other states, don't add to queue but still store the task
		b.logger.Info("Task submitted with non-pending state",
			zap.String("task_id", taskID),
			zap.String("state", string(taskCopy.GetState())))
	}

	return nil
}

// GetTask retrieves a task by its ID
func (b *MemoryBroker) GetTask(ctx context.Context, id string) (*task.Task, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, exists := b.tasks[id]
	if !exists {
		return nil, broker.ErrTaskNotFound
	}

	// Return a clone to prevent external modification
	return t.Clone(), nil
}

// isShuttingDown checks if the broker is shutting down
func (b *MemoryBroker) isShuttingDown() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.shuttingDown
}
