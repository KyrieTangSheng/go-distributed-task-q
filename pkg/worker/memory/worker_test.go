package memory

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mockBroker implements a simple mock of the broker.Broker interface for testing
type mockBroker struct {
	tasks        map[string]*task.Task
	nextTaskFunc func() (*task.Task, error)
	completeFunc func(id string, result []byte) error
	failFunc     func(id string, errMsg string) error
	retryFunc    func(id string, backoffSeconds int) error
}

func newMockBroker() *mockBroker {
	return &mockBroker{
		tasks: make(map[string]*task.Task),
		nextTaskFunc: func() (*task.Task, error) {
			return nil, broker.ErrNoTaskAvailable
		},
		completeFunc: func(id string, result []byte) error {
			return nil
		},
		failFunc: func(id string, errMsg string) error {
			return nil
		},
		retryFunc: func(id string, backoffSeconds int) error {
			return nil
		},
	}
}

func (m *mockBroker) SubmitTask(ctx context.Context, t *task.Task) error {
	m.tasks[t.GetID()] = t
	return nil
}

func (m *mockBroker) GetTask(ctx context.Context, id string) (*task.Task, error) {
	t, ok := m.tasks[id]
	if !ok {
		return nil, broker.ErrTaskNotFound
	}
	return t, nil
}

func (m *mockBroker) NextTask(ctx context.Context) (*task.Task, error) {
	return m.nextTaskFunc()
}

func (m *mockBroker) CompleteTask(ctx context.Context, id string, result []byte) error {
	return m.completeFunc(id, result)
}

func (m *mockBroker) FailTask(ctx context.Context, id string, errMsg string) error {
	return m.failFunc(id, errMsg)
}

func (m *mockBroker) RetryTask(ctx context.Context, id string, backoffSeconds int) error {
	return m.retryFunc(id, backoffSeconds)
}

func (m *mockBroker) GetDeadLetterTasks(ctx context.Context) ([]*task.Task, error) {
	return nil, nil
}

func (m *mockBroker) GetDeadLetterTask(ctx context.Context, id string) (*task.Task, error) {
	return nil, nil
}

func (m *mockBroker) RetryDeadLetterTask(ctx context.Context, id string) error {
	return nil
}

func (m *mockBroker) DeleteDeadLetterTask(ctx context.Context, id string) error {
	return nil
}

func (m *mockBroker) Stats(ctx context.Context) (broker.Stats, error) {
	return broker.Stats{}, nil
}

func (m *mockBroker) Shutdown(ctx context.Context) error {
	return nil
}

func (m *mockBroker) MoveToDeadLetter(ctx context.Context, id string, reason string) error {
	return nil
}

// TestWorkerRegistration tests handler registration
func TestWorkerRegistration(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	mockBroker := newMockBroker()
	w := NewWorker(mockBroker, Options{Logger: logger})

	// Test successful registration
	err := w.RegisterHandler("test", func(ctx context.Context, task *task.Task) ([]byte, error) {
		return []byte("success"), nil
	})
	assert.NoError(t, err)

	// Test duplicate registration
	err = w.RegisterHandler("test", func(ctx context.Context, task *task.Task) ([]byte, error) {
		return []byte("success"), nil
	})
	assert.Equal(t, worker.ErrHandlerAlreadyRegistered, err)

	// Test registration after shutdown
	w.shuttingDown = true
	err = w.RegisterHandler("test2", func(ctx context.Context, task *task.Task) ([]byte, error) {
		return []byte("success"), nil
	})
	assert.Equal(t, worker.ErrWorkerStopped, err)
}

// TestWorkerStartStop tests worker start and stop
func TestWorkerStartStop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	mockBroker := newMockBroker()
	w := NewWorker(mockBroker, Options{Logger: logger})

	// Test start
	err := w.Start(context.Background())
	assert.NoError(t, err)

	// Test double start
	err = w.Start(context.Background())
	assert.Equal(t, worker.ErrWorkerAlreadyRunning, err)

	// Test stop
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = w.Stop(ctx)
	assert.NoError(t, err)

	// Test double stop (should be no-op)
	err = w.Stop(ctx)
	assert.NoError(t, err)
}

// TestWorkerProcessTask tests task processing
func TestWorkerProcessTask(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	mockBroker := newMockBroker()
	w := NewWorker(mockBroker, Options{Logger: logger})

	// Register a handler
	handlerCalled := false
	err := w.RegisterHandler("test", func(ctx context.Context, task *task.Task) ([]byte, error) {
		handlerCalled = true
		return []byte("success"), nil
	})
	require.NoError(t, err)

	// Create a task
	testTask := task.NewTask("test", []byte(`{"key":"value"}`))
	testTask.MarkStarted()

	// Set up the mock broker to return our task
	completeCalled := false
	mockBroker.completeFunc = func(id string, result []byte) error {
		completeCalled = true
		assert.Equal(t, testTask.GetID(), id)
		assert.Equal(t, []byte("success"), result)
		return nil
	}

	// Process the task
	w.processTask(context.Background(), testTask)

	// Verify handler and complete were called
	assert.True(t, handlerCalled)
	assert.True(t, completeCalled)
}

// TestWorkerProcessTaskError tests task processing with errors
func TestWorkerProcessTaskError(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	mockBroker := newMockBroker()
	w := NewWorker(mockBroker, Options{Logger: logger})

	// Register a handler that returns an error
	err := w.RegisterHandler("test", func(ctx context.Context, task *task.Task) ([]byte, error) {
		return nil, errors.New("test error")
	})
	require.NoError(t, err)

	// Create a task
	testTask := task.NewTask("test", []byte(`{"key":"value"}`))
	testTask.MarkStarted()

	// Set up the mock broker to handle failure
	failCalled := false
	mockBroker.failFunc = func(id string, errMsg string) error {
		failCalled = true
		assert.Equal(t, testTask.GetID(), id)
		assert.Equal(t, "test error", errMsg)
		return nil
	}

	// Process the task
	w.processTask(context.Background(), testTask)

	// Verify fail was called
	assert.True(t, failCalled)
}

// TestWorkerProcessTaskRetryableError tests task processing with retryable errors
func TestWorkerProcessTaskRetryableError(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	mockBroker := newMockBroker()
	w := NewWorker(mockBroker, Options{Logger: logger})

	// Register a handler that returns a retryable error
	err := w.RegisterHandler("test", func(ctx context.Context, task *task.Task) ([]byte, error) {
		return nil, worker.NewRetryableError(errors.New("temporary error"))
	})
	require.NoError(t, err)

	// Create a task
	testTask := task.NewTask("test", []byte(`{"key":"value"}`))
	testTask.MarkStarted()

	// Set up the mock broker to handle retry
	retryCalled := false
	mockBroker.retryFunc = func(id string, backoffSeconds int) error {
		retryCalled = true
		assert.Equal(t, testTask.GetID(), id)
		assert.Greater(t, backoffSeconds, 0)
		return nil
	}

	// Process the task
	w.processTask(context.Background(), testTask)

	// Verify retry was called
	assert.True(t, retryCalled)
}

// TestWorkerStats tests worker statistics
func TestWorkerStats(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	mockBroker := newMockBroker()
	w := NewWorker(mockBroker, Options{Logger: logger})

	// Get initial stats
	stats, err := w.Stats(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, stats.ActiveTasks)
	assert.Equal(t, uint64(0), stats.CompletedTasks)
	assert.Equal(t, uint64(0), stats.FailedTasks)

	// Update some metrics
	atomic.AddUint64(&w.metrics.tasksCompleted, 5)
	atomic.AddUint64(&w.metrics.tasksFailed, 2)
	atomic.AddInt32(&w.activeTasks, 3)

	// Record some processing times
	w.metrics.processingTime.Record(100 * time.Millisecond)
	w.metrics.processingTime.Record(200 * time.Millisecond)

	// Get updated stats
	stats, err = w.Stats(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, stats.ActiveTasks)
	assert.Equal(t, uint64(5), stats.CompletedTasks)
	assert.Equal(t, uint64(2), stats.FailedTasks)
	assert.Equal(t, int64(150), stats.AvgProcessingTimeMs) // (100+200)/2
	assert.Equal(t, int64(100), stats.MinProcessingTimeMs)
	assert.Equal(t, int64(200), stats.MaxProcessingTimeMs)

	// Test stats after shutdown
	w.shuttingDown = true
	_, err = w.Stats(context.Background())
	assert.Equal(t, worker.ErrWorkerStopped, err)
}
