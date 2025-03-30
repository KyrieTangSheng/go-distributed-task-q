package worker

import (
	"context"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
)

// Handler is a function that processes a specific task type
type Handler func(ctx context.Context, task *task.Task) ([]byte, error)

// Worker defines the interface for task execution workers.
// It handles task processing, handler registration, and lifecycle management.
type Worker interface {
	// RegisterHandler registers a handler for a specific task type
	RegisterHandler(taskType string, handler Handler) error

	// Start begins task processing
	Start(ctx context.Context) error

	// Stop gracefully stops the worker, allowing in-progress tasks to complete
	Stop(ctx context.Context) error

	// Stats returns current worker statistics
	Stats(ctx context.Context) (Stats, error)
}

// Stats represents worker statistics
type Stats struct {
	ActiveTasks         int    `json:"active_tasks"`
	CompletedTasks      uint64 `json:"completed_tasks"`
	FailedTasks         uint64 `json:"failed_tasks"`
	AvgProcessingTimeMs int64  `json:"avg_processing_time_ms"`
	MaxProcessingTimeMs int64  `json:"max_processing_time_ms"`
	MinProcessingTimeMs int64  `json:"min_processing_time_ms"`
	UpdatedAt           string `json:"updated_at"`
}

// Standard errors returned by worker implementations
var (
	ErrHandlerAlreadyRegistered = NewError("handler already registered for task type")
	ErrHandlerNotFound          = NewError("no handler registered for task type")
	ErrWorkerStopped            = NewError("worker has been stopped")
	ErrWorkerAlreadyRunning     = NewError("worker is already running")
)

// Error represents a worker error
type Error struct {
	msg string
}

// NewError creates a new worker error
func NewError(msg string) *Error {
	return &Error{msg: msg}
}

func (e *Error) Error() string {
	return e.msg
}

// RetryableError is an error that indicates a task should be retried
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	return e.Err.Error()
}

// NewRetryableError wraps an error to indicate it should trigger a retry
func NewRetryableError(err error) *RetryableError {
	return &RetryableError{Err: err}
}

// IsRetryable checks if an error is retryable
func IsRetryable(err error) bool {
	_, ok := err.(*RetryableError)
	return ok
}
