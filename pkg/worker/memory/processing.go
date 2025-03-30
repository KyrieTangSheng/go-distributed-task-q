package memory

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/worker"
	"go.uber.org/zap"
)

// processTask handles the execution of a task
func (w *MemoryWorker) processTask(ctx context.Context, t *task.Task) {
	taskID := t.GetID()
	taskType := t.GetType()

	// Get the handler for this task type
	handler, err := w.getHandler(taskType)
	if err != nil {
		w.logger.Error("No handler registered for task type",
			zap.String("task_id", taskID),
			zap.String("task_type", taskType))

		// Mark as failed
		if err := w.broker.FailTask(ctx, taskID, "No handler registered for task type: "+taskType); err != nil {
			w.logger.Error("Failed to mark task as failed",
				zap.String("task_id", taskID),
				zap.Error(err))
		}

		atomic.AddUint64(&w.metrics.tasksFailed, 1)
		return
	}

	// Create a task-specific context that we can cancel
	taskCtx, cancel := context.WithTimeout(ctx, time.Duration(t.GetTimeout())*time.Second)
	defer cancel()

	w.logger.Info("Processing task",
		zap.String("task_id", taskID),
		zap.String("task_type", taskType))

	// Execute the handler and measure time
	startTime := time.Now()
	result, err := handler(taskCtx, t)
	duration := time.Since(startTime)

	// Record the processing time
	w.metrics.processingTime.Record(duration)

	// Handle the result
	if err != nil {
		w.logger.Warn("Task execution failed",
			zap.String("task_id", taskID),
			zap.String("task_type", taskType),
			zap.Error(err),
			zap.Duration("duration", duration))

		w.logger.Debug("Checking if error is retryable",
			zap.String("task_id", taskID),
			zap.Error(err),
			zap.Bool("is_retryable", worker.IsRetryable(err)))

		// For retryable errors, mark as failed and retry
		if failErr := w.broker.FailTask(ctx, taskID, err.Error()); failErr != nil {
			w.logger.Error("Failed to mark task as failed",
				zap.String("task_id", taskID),
				zap.Error(failErr))
			return
		}
		// Handle based on whether the error is retryable
		if worker.IsRetryable(err) {

			// Calculate backoff (exponential with jitter)
			backoff := calculateBackoff(t.GetRetryCount())

			if retryErr := w.broker.RetryTask(ctx, taskID, backoff); retryErr != nil {
				w.logger.Error("Failed to retry task",
					zap.String("task_id", taskID),
					zap.Error(retryErr))
			}
		} else {
			// For non-retryable errors, move directly to dead letter queue
			w.logger.Info("Moving non-retryable task to dead letter queue",
				zap.String("task_id", taskID),
				zap.Error(err))

			if moveErr := w.broker.MoveToDeadLetter(ctx, taskID, "Non-retryable error: "+err.Error()); moveErr != nil {
				w.logger.Error("Failed to move task to dead letter queue",
					zap.String("task_id", taskID),
					zap.Error(moveErr))
			}
			atomic.AddUint64(&w.metrics.tasksFailed, 1)
		}
	} else {
		// Task completed successfully
		w.logger.Info("Task completed successfully",
			zap.String("task_id", taskID),
			zap.String("task_type", taskType),
			zap.Duration("duration", duration))

		if err := w.broker.CompleteTask(ctx, taskID, result); err != nil {
			w.logger.Error("Failed to mark task as completed",
				zap.String("task_id", taskID),
				zap.Error(err))
		}

		atomic.AddUint64(&w.metrics.tasksCompleted, 1)
	}
}

// calculateBackoff calculates the backoff time in seconds based on retry count
// Uses exponential backoff with jitter
func calculateBackoff(retryCount int) int {
	// Base backoff: 1s, 2s, 4s, 8s, 16s, 32s, 60s (max)
	backoff := 1 << uint(retryCount)
	if backoff > 60 {
		backoff = 60
	}

	// Add jitter (±20%)
	jitter := backoff / 5
	if jitter < 1 {
		jitter = 1
	}

	// Simple jitter implementation
	if time.Now().UnixNano()%2 == 0 {
		backoff += jitter
	} else {
		backoff -= jitter
	}

	return backoff
}

// min returns the minimum of two durations
func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
