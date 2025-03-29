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

	// Trigger persistence checkpoint if enabled
	if b.persistence != nil {
		go func() {
			if err := b.persistence.SaveState(); err != nil {
				b.logger.Warn("Failed to save state after task completion",
					zap.Error(err))
			}
		}()
	}

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

	// Check if we should retry or move to dead letter queue
	if t.ShouldRetry() {
		b.logger.Info("Task failed but will be retried",
			zap.String("task_id", id),
			zap.String("task_type", t.GetType()),
			zap.String("error", errMsg),
			zap.Int("retry_count", t.GetRetryCount()),
			zap.Int("max_retries", t.GetMaxRetries()))
	} else {
		// Move to dead letter queue
		err = t.MarkDead("Max retries exceeded: " + errMsg)
		if err != nil {
			b.logger.Warn("Failed to mark task as dead",
				zap.String("task_id", id),
				zap.Error(err))
			return err
		}

		// Add to dead letter queue
		b.deadLetterQueue.Add(t.Clone())

		b.logger.Info("Task moved to dead letter queue after max retries",
			zap.String("task_id", id),
			zap.String("task_type", t.GetType()),
			zap.String("error", errMsg),
			zap.Int("retry_count", t.GetRetryCount()),
			zap.Int("max_retries", t.GetMaxRetries()))
	}

	// Trigger persistence checkpoint if enabled
	if b.persistence != nil {
		go func() {
			if err := b.persistence.SaveState(); err != nil {
				b.logger.Warn("Failed to save state after task failure",
					zap.Error(err))
			}
		}()
	}

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

	// Trigger persistence checkpoint if enabled
	if b.persistence != nil {
		go func() {
			if err := b.persistence.SaveState(); err != nil {
				b.logger.Warn("Failed to save state after task retry",
					zap.Error(err))
			}
		}()
	}

	return nil
}
