package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/worker"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
)

func TestLoggingMiddleware(t *testing.T) {
	// Create a logger that records logs
	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(core)

	// Create a simple handler
	baseHandler := func(ctx context.Context, task *task.Task) ([]byte, error) {
		time.Sleep(10 * time.Millisecond) // Simulate work
		return []byte("result"), nil
	}

	// Wrap with logging middleware
	handler := Logging(logger)(baseHandler)

	// Create a task
	testTask := task.NewTask("test", []byte(`{"key":"value"}`))

	// Execute the handler
	result, err := handler(context.Background(), testTask)

	// Verify result
	assert.NoError(t, err)
	assert.Equal(t, []byte("result"), result)

	// Verify logs
	entries := logs.All()
	assert.Equal(t, 2, len(entries))
	assert.Contains(t, entries[0].Message, "Starting task execution")
	assert.Contains(t, entries[1].Message, "Task execution completed")
}

func TestRecoveryMiddleware(t *testing.T) {
	// Create a logger
	logger := zaptest.NewLogger(t)

	// Create a handler that panics
	panicHandler := func(ctx context.Context, task *task.Task) ([]byte, error) {
		panic("test panic")
	}

	// Wrap with recovery middleware
	handler := Recovery(logger)(panicHandler)

	// Create a task
	testTask := task.NewTask("test", []byte(`{"key":"value"}`))

	// Execute the handler
	result, err := handler(context.Background(), testTask)

	// Verify result
	assert.Nil(t, result)
	assert.Error(t, err)

	// Verify it's a retryable error
	assert.True(t, worker.IsRetryable(err))

	// Verify the error message
	assert.Contains(t, err.Error(), "panic: test panic")
}

func TestChainMiddleware(t *testing.T) {
	// Create middleware that counts calls
	var count int
	countMiddleware := func(next worker.Handler) worker.Handler {
		return func(ctx context.Context, task *task.Task) ([]byte, error) {
			count++
			return next(ctx, task)
		}
	}

	// Create middleware that adds a prefix to results
	prefixMiddleware := func(next worker.Handler) worker.Handler {
		return func(ctx context.Context, task *task.Task) ([]byte, error) {
			result, err := next(ctx, task)
			if err != nil {
				return nil, err
			}
			return append([]byte("prefix-"), result...), nil
		}
	}

	// Create a base handler
	baseHandler := func(ctx context.Context, task *task.Task) ([]byte, error) {
		return []byte("result"), nil
	}

	// Chain the middleware
	handler := Chain(countMiddleware, prefixMiddleware)(baseHandler)

	// Create a task
	testTask := task.NewTask("test", []byte(`{"key":"value"}`))

	// Execute the handler
	result, err := handler(context.Background(), testTask)

	// Verify result
	assert.NoError(t, err)
	assert.Equal(t, []byte("prefix-result"), result)
	assert.Equal(t, 1, count)
}
