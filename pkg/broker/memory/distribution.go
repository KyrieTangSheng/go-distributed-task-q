package memory

import (
	"context"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"go.uber.org/zap"
)

// NextTask retrieves the next available task for processing
func (b *MemoryBroker) NextTask(ctx context.Context) (*task.Task, error) {
	// Check if broker is shutting down
	if b.isShuttingDown() {
		return nil, broker.ErrBrokerClosed
	}

	// Try to get a task ID from the queue
	taskID, found := b.pendingQueue.PopTask()
	if !found {
		return nil, broker.ErrNoTaskAvailable
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Get the task from storage
	t, exists := b.tasks[taskID]
	if !exists {
		// This should not happen, but handle it gracefully
		b.logger.Warn("Task in queue not found in storage",
			zap.String("task_id", taskID))
		return nil, broker.ErrTaskNotFound
	}

	// Mark the task as running
	err := t.MarkStarted()
	if err != nil {
		// If we can't mark it as started, requeue it
		b.logger.Warn("Failed to mark task as started, requeuing",
			zap.String("task_id", taskID),
			zap.Error(err))
		b.pendingQueue.PushTask(taskID, t.GetPriority(), time.Now())
		return nil, err
	}

	// Update metrics
	b.metrics.QueueDepth = int64(b.pendingQueue.Len())

	b.logger.Info("Task dequeued for processing",
		zap.String("task_id", taskID),
		zap.String("task_type", t.GetType()),
		zap.Int("queue_depth", b.pendingQueue.Len()))

	// Return a clone to prevent external modification
	return t.Clone(), nil
}
