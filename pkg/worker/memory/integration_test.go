package memory

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker/memory"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestWorkerIntegration tests the worker with a real broker
func TestWorkerIntegration(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	// Create a broker
	broker := memory.NewBroker(memory.Options{
		MaxConcurrent: 10,
		Logger:        logger,
	})

	// Create a worker
	w := NewWorker(broker, Options{
		MaxConcurrent: 5,
		PollInterval:  10 * time.Millisecond,
		BackoffMin:    10 * time.Millisecond,
		BackoffMax:    100 * time.Millisecond,
		Logger:        logger,
	})

	// Register handlers
	successHandler := func(ctx context.Context, t *task.Task) ([]byte, error) {
		var payload map[string]interface{}
		if err := json.Unmarshal(t.GetPayload(), &payload); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]interface{}{
			"result": "success",
			"input":  payload,
		})
	}

	failHandler := func(ctx context.Context, t *task.Task) ([]byte, error) {
		return nil, errors.New("intentional failure")
	}

	retryHandler := func(ctx context.Context, t *task.Task) ([]byte, error) {
		// Succeed on the second try
		if t.GetRetryCount() > 0 {
			return []byte("retry succeeded"), nil
		}
		return nil, worker.NewRetryableError(errors.New("temporary failure"))
	}

	require.NoError(t, w.RegisterHandler("success", successHandler))
	require.NoError(t, w.RegisterHandler("fail", failHandler))
	require.NoError(t, w.RegisterHandler("retry", retryHandler))

	// Start the worker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, w.Start(ctx))

	// Submit tasks
	var wg sync.WaitGroup
	wg.Add(3)

	// Success task
	successTask := task.NewTask("success", json.RawMessage(`{"test":"value"}`))
	require.NoError(t, broker.SubmitTask(ctx, successTask))

	// Failure task
	failTask := task.NewTask("fail", json.RawMessage(`{"test":"value"}`))
	require.NoError(t, broker.SubmitTask(ctx, failTask))

	// Retry task
	retryTask := task.NewTask("retry", json.RawMessage(`{"test":"value"}`))
	require.NoError(t, broker.SubmitTask(ctx, retryTask))

	// Wait for tasks to be processed (with timeout)
	tasksDone := make(chan struct{})
	go func() {
		// Poll until all tasks are in terminal states
		for {
			time.Sleep(50 * time.Millisecond)

			successT, _ := broker.GetTask(ctx, successTask.GetID())
			failT, _ := broker.GetTask(ctx, failTask.GetID())
			retryT, _ := broker.GetTask(ctx, retryTask.GetID())

			if successT != nil && successT.GetState() == task.StateCompleted &&
				failT != nil && failT.GetState() == task.StateDead &&
				retryT != nil && retryT.GetState() == task.StateCompleted {
				close(tasksDone)
				return
			}
		}
	}()

	select {
	case <-tasksDone:
		// Tasks completed
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for tasks to complete")
	}

	// Verify task states
	successT, err := broker.GetTask(ctx, successTask.GetID())
	require.NoError(t, err)
	assert.Equal(t, task.StateCompleted, successT.GetState())

	failT, err := broker.GetTask(ctx, failTask.GetID())
	require.NoError(t, err)
	assert.Equal(t, task.StateDead, failT.GetState())

	retryT, err := broker.GetTask(ctx, retryTask.GetID())
	require.NoError(t, err)
	assert.Equal(t, task.StateCompleted, retryT.GetState())
	assert.Equal(t, 1, retryT.GetRetryCount())

	// Check worker stats
	stats, err := w.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), stats.CompletedTasks)
	assert.Equal(t, uint64(1), stats.FailedTasks)

	// Shutdown
	require.NoError(t, w.Stop(ctx))
	require.NoError(t, broker.Shutdown(ctx))
}
