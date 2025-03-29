package memory

import (
	"context"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker"
	"go.uber.org/zap"
)

// CompleteTask marks a task as successfully completed
func (b *MemoryBroker) CompleteTask(ctx context.Context, id string, result []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Find the task
	t, exists := b.tasks[id]
	if !exists {
		return broker.ErrTaskNotFound
	}

	// Calculate processing time for metrics
	if startedAt := t.GetStartedAt(); startedAt != nil {
		processingTime := time.Since(*startedAt)
		b.metrics.ProcessingTime.Record(processingTime)
	}

	// Mark as completed
	err := t.MarkCompleted()
	if err != nil {
		b.logger.Warn("Failed to mark task as completed",
			zap.String("task_id", id),
			zap.Error(err))
		return err
	}

	// Store result if provided
	if len(result) > 0 {
		t.StoreResultInfo(string(result))
	}

	// Update metrics
	b.metrics.TasksCompleted++

	b.logger.Info("Task completed",
		zap.String("task_id", id),
		zap.String("task_type", t.GetType()),
		zap.Int("result_size", len(result)))

	return nil
}

// FailTask marks a task as failed
func (b *MemoryBroker) FailTask(ctx context.Context, id string, errMsg string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Find the task
	t, exists := b.tasks[id]
	if !exists {
		return broker.ErrTaskNotFound
	}

	// Mark as failed
	err := t.MarkFailed(errMsg)
	if err != nil {
		b.logger.Warn("Failed to mark task as failed",
			zap.String("task_id", id),
			zap.Error(err))
		return err
	}

	// Update metrics
	b.metrics.TasksFailed++

	b.logger.Info("Task failed",
		zap.String("task_id", id),
		zap.String("task_type", t.GetType()),
		zap.String("error", errMsg))

	return nil
}

// RetryTask schedules a failed task for retry
func (b *MemoryBroker) RetryTask(ctx context.Context, id string, backoffSeconds int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Find the task
	t, exists := b.tasks[id]
	if !exists {
		return broker.ErrTaskNotFound
	}

	// Check if we can retry
	if !t.ShouldRetry() {
		b.logger.Warn("Task has exceeded retry limit",
			zap.String("task_id", id),
			zap.Int("retry_count", t.GetRetryCount()),
			zap.Int("max_retries", t.GetMaxRetries()))
		return broker.ErrMaxRetriesExceeded
	}

	// Calculate retry time
	retryTime := time.Now().Add(time.Duration(backoffSeconds) * time.Second)

	// Prepare for retry
	err := t.PrepareForRetry(backoffSeconds)
	if err != nil {
		b.logger.Warn("Failed to prepare task for retry",
			zap.String("task_id", id),
			zap.Error(err))
		return err
	}

	// Add back to queue with the retry time
	b.pendingQueue.PushTask(id, t.GetPriority(), retryTime)

	// Update metrics
	b.metrics.TasksRetried++
	b.metrics.QueueDepth = int64(b.pendingQueue.Len())

	b.logger.Info("Task scheduled for retry",
		zap.String("task_id", id),
		zap.String("task_type", t.GetType()),
		zap.Time("retry_time", retryTime),
		zap.Int("retry_count", t.GetRetryCount()),
		zap.Int("backoff_seconds", backoffSeconds))

	return nil
}
