package middleware

import (
	"context"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/worker"
	"go.uber.org/zap"
)

// Middleware wraps a task handler with additional functionality
type Middleware func(worker.Handler) worker.Handler

// Chain combines multiple middleware into a single middleware
func Chain(middlewares ...Middleware) Middleware {
	return func(next worker.Handler) worker.Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](next)
		}
		return next
	}
}

// Logging creates a middleware that logs task execution details
func Logging(logger *zap.Logger) Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(ctx context.Context, t *task.Task) ([]byte, error) {
			taskID := t.GetID()
			taskType := t.GetType()

			logger.Info("Starting task execution",
				zap.String("task_id", taskID),
				zap.String("task_type", taskType))

			startTime := time.Now()
			result, err := next(ctx, t)
			duration := time.Since(startTime)

			if err != nil {
				logger.Warn("Task execution failed",
					zap.String("task_id", taskID),
					zap.String("task_type", taskType),
					zap.Error(err),
					zap.Duration("duration", duration))
			} else {
				logger.Info("Task execution completed",
					zap.String("task_id", taskID),
					zap.String("task_type", taskType),
					zap.Duration("duration", duration))
			}

			return result, err
		}
	}
}

// Recovery creates a middleware that recovers from panics
func Recovery(logger *zap.Logger) Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(ctx context.Context, t *task.Task) (result []byte, err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("Panic recovered in task execution",
						zap.String("task_id", t.GetID()),
						zap.String("task_type", t.GetType()),
						zap.Any("panic", r))

					// Convert panic to error
					switch x := r.(type) {
					case string:
						err = worker.NewRetryableError(
							&PanicError{Msg: x})
					case error:
						err = worker.NewRetryableError(
							&PanicError{Err: x})
					default:
						err = worker.NewRetryableError(
							&PanicError{Msg: "unknown panic"})
					}
				}
			}()

			return next(ctx, t)
		}
	}
}

// PanicError represents a panic that occurred during task execution
type PanicError struct {
	Msg string
	Err error
}

func (e *PanicError) Error() string {
	if e.Err != nil {
		return "panic: " + e.Err.Error()
	}
	return "panic: " + e.Msg
}
