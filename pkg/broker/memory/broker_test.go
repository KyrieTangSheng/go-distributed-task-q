package memory

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestBroker_SubmitAndGetTask(t *testing.T) {
	// Setup
	b := NewBroker(Options{Logger: zap.NewNop()})
	ctx := context.Background()

	// Create a task
	payload := json.RawMessage(`{"test":"data"}`)
	task := task.NewTask("test-type", payload)

	// Submit the task
	err := b.SubmitTask(ctx, task)
	require.NoError(t, err)

	// Retrieve the task
	retrieved, err := b.GetTask(ctx, task.GetID())
	require.NoError(t, err)
	assert.Equal(t, task.GetID(), retrieved.GetID())
	assert.Equal(t, task.GetType(), retrieved.GetType())
	assert.Equal(t, string(task.GetPayload()), string(retrieved.GetPayload()))
}

func TestBroker_IdempotencyKey(t *testing.T) {
	// Setup
	b := NewBroker(Options{Logger: zap.NewNop()})
	ctx := context.Background()

	// Create a task with idempotency key
	payload := json.RawMessage(`{"test":"data"}`)
	task1 := task.NewTask("test-type", payload, task.WithIdempotencyKey("test-key"))

	// Submit the task
	err := b.SubmitTask(ctx, task1)
	require.NoError(t, err)

	// Create another task with the same idempotency key
	task2 := task.NewTask("test-type", payload, task.WithIdempotencyKey("test-key"))

	// Submit the second task - should fail
	err = b.SubmitTask(ctx, task2)
	assert.True(t, errors.Is(err, broker.ErrDuplicateTask))
}

func TestBroker_NextTask(t *testing.T) {
	// Setup
	b := NewBroker(Options{Logger: zap.NewNop()})
	ctx := context.Background()

	// Create and submit tasks with different priorities
	payload := json.RawMessage(`{"test":"data"}`)
	task1 := task.NewTask("test-type", payload, task.WithPriority(1))
	task2 := task.NewTask("test-type", payload, task.WithPriority(2))
	task3 := task.NewTask("test-type", payload, task.WithPriority(3))

	err := b.SubmitTask(ctx, task1)
	require.NoError(t, err)
	err = b.SubmitTask(ctx, task2)
	require.NoError(t, err)
	err = b.SubmitTask(ctx, task3)
	require.NoError(t, err)

	// Get next task - should be task3 (highest priority)
	next, err := b.NextTask(ctx)
	require.NoError(t, err)
	assert.Equal(t, task3.GetID(), next.GetID())
	assert.Equal(t, task.StateRunning, next.GetState())

	// Get next task - should be task2 (second highest priority)
	next, err = b.NextTask(ctx)
	require.NoError(t, err)
	assert.Equal(t, task2.GetID(), next.GetID())

	// Get next task - should be task1 (lowest priority)
	next, err = b.NextTask(ctx)
	require.NoError(t, err)
	assert.Equal(t, task1.GetID(), next.GetID())

	// Get next task - should return ErrNoTaskAvailable
	_, err = b.NextTask(ctx)
	assert.True(t, errors.Is(err, broker.ErrNoTaskAvailable))
}

func TestBroker_TaskLifecycle(t *testing.T) {
	// Setup
	b := NewBroker(Options{Logger: zap.NewNop()})
	ctx := context.Background()

	// Create and submit a task
	payload := json.RawMessage(`{"test":"data"}`)
	taskobj := task.NewTask("test-type", payload)

	err := b.SubmitTask(ctx, taskobj)
	require.NoError(t, err)

	// Get the task
	next, err := b.NextTask(ctx)
	require.NoError(t, err)
	assert.Equal(t, taskobj.GetID(), next.GetID())
	assert.Equal(t, task.StateRunning, next.GetState())

	// Complete the task
	result := []byte(`{"result":"success"}`)
	err = b.CompleteTask(ctx, next.GetID(), result)
	require.NoError(t, err)

	// Verify task state
	completed, err := b.GetTask(ctx, next.GetID())
	require.NoError(t, err)
	assert.Equal(t, task.StateCompleted, completed.GetState())
}

func TestBroker_FailAndRetryTask(t *testing.T) {
	// Setup
	b := NewBroker(Options{Logger: zap.NewNop()})
	ctx := context.Background()

	// Create and submit a task with retry configuration
	payload := json.RawMessage(`{"test":"data"}`)
	taskobj := task.NewTask("test-type", payload, task.WithMaxRetries(2))

	err := b.SubmitTask(ctx, taskobj)
	require.NoError(t, err)

	// Get the task
	next, err := b.NextTask(ctx)
	require.NoError(t, err)
	taskID := next.GetID()

	// Fail the task
	err = b.FailTask(ctx, taskID, "test failure")
	require.NoError(t, err)

	// Verify task state
	failed, err := b.GetTask(ctx, taskID)
	require.NoError(t, err)
	assert.Equal(t, task.StateFailed, failed.GetState())

	// Retry the task
	err = b.RetryTask(ctx, taskID, 1)
	require.NoError(t, err)

	// Verify task is back in the queue
	// Wait a bit for the retry delay
	time.Sleep(1100 * time.Millisecond)

	// Get the task again
	retried, err := b.NextTask(ctx)
	require.NoError(t, err)
	assert.Equal(t, taskID, retried.GetID())
	assert.Equal(t, task.StateRunning, retried.GetState())
	assert.Equal(t, 1, retried.GetRetryCount())

	// Fail again
	err = b.FailTask(ctx, taskID, "test failure 2")
	require.NoError(t, err)

	// Retry again
	err = b.RetryTask(ctx, taskID, 1)
	require.NoError(t, err)

	// Wait for retry delay
	time.Sleep(1100 * time.Millisecond)

	// Get the task again
	retried, err = b.NextTask(ctx)
	require.NoError(t, err)
	assert.Equal(t, taskID, retried.GetID())
	assert.Equal(t, 2, retried.GetRetryCount())

	// Fail one more time
	err = b.FailTask(ctx, taskID, "test failure 3")
	require.NoError(t, err)

	// Try to retry - should fail due to max retries
	err = b.RetryTask(ctx, taskID, 1)
	assert.True(t, errors.Is(err, broker.ErrMaxRetriesExceeded))
}

func TestBroker_Stats(t *testing.T) {
	// Setup
	b := NewBroker(Options{Logger: zap.NewNop()})
	ctx := context.Background()

	// Get initial stats
	stats, err := b.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, stats.Total)

	// Create and submit tasks
	payload := json.RawMessage(`{"test":"data"}`)
	task1 := task.NewTask("test-type", payload)
	task2 := task.NewTask("test-type", payload)
	task3 := task.NewTask("test-type", payload)

	err = b.SubmitTask(ctx, task1)
	require.NoError(t, err)
	err = b.SubmitTask(ctx, task2)
	require.NoError(t, err)
	err = b.SubmitTask(ctx, task3)
	require.NoError(t, err)

	// Get stats after submission
	stats, err = b.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, stats.Total)
	assert.Equal(t, 3, stats.Pending)

	// Get a task
	next, err := b.NextTask(ctx)
	require.NoError(t, err)

	// Get stats after getting a task
	stats, err = b.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, stats.Total)
	assert.Equal(t, 2, stats.Pending)
	assert.Equal(t, 1, stats.Running)

	// Complete the task
	err = b.CompleteTask(ctx, next.GetID(), nil)
	require.NoError(t, err)

	// Get stats after completion
	stats, err = b.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, stats.Total)
	assert.Equal(t, 2, stats.Pending)
	assert.Equal(t, 0, stats.Running)
	assert.Equal(t, 1, stats.Completed)
}

func TestBroker_Shutdown(t *testing.T) {
	// Setup
	b := NewBroker(Options{Logger: zap.NewNop()})
	ctx := context.Background()

	// Create and submit a task
	payload := json.RawMessage(`{"test":"data"}`)
	task := task.NewTask("test-type", payload)

	err := b.SubmitTask(ctx, task)
	require.NoError(t, err)

	// Start shutdown
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Shutdown should complete quickly since no tasks are running
	err = b.Shutdown(shutdownCtx)
	require.NoError(t, err)

	// Try to submit a task after shutdown
	err = b.SubmitTask(ctx, task)
	assert.True(t, errors.Is(err, broker.ErrBrokerClosed))

	// Try to get a task after shutdown
	_, err = b.NextTask(ctx)
	assert.True(t, errors.Is(err, broker.ErrBrokerClosed))
}

func TestBroker_ShutdownWithRunningTasks(t *testing.T) {
	// Setup
	b := NewBroker(Options{Logger: zap.NewNop()})
	ctx := context.Background()

	// Create and submit a task
	payload := json.RawMessage(`{"test":"data"}`)
	task := task.NewTask("test-type", payload)

	err := b.SubmitTask(ctx, task)
	require.NoError(t, err)

	// Get the task to mark it as running
	next, err := b.NextTask(ctx)
	require.NoError(t, err)

	// Start shutdown in a goroutine
	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	shutdownDone := make(chan error)
	go func() {
		shutdownDone <- b.Shutdown(shutdownCtx)
	}()

	// Wait a bit to ensure shutdown has started
	time.Sleep(500 * time.Millisecond)

	// Complete the task
	err = b.CompleteTask(ctx, next.GetID(), nil)
	require.NoError(t, err)

	// Wait for shutdown to complete
	err = <-shutdownDone
	require.NoError(t, err)
}
