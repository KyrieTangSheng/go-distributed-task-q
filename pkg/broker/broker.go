package broker

import (
	"context"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
)

// Broker defines the interface for task queue brokers.
// It handles task submission, distribution, and lifecycle management.
type Broker interface {
	// SubmitTask adds a new task to the queue
	// Returns ErrDuplicateTask if a task with the same idempotency key exists
	SubmitTask(ctx context.Context, task *task.Task) error

	// GetTask retrieves a task by its ID
	// Returns ErrTaskNotFound if the task doesn't exist
	GetTask(ctx context.Context, id string) (*task.Task, error)

	// NextTask retrieves the next available task for processing
	// Returns ErrNoTaskAvailable if no tasks are available
	NextTask(ctx context.Context) (*task.Task, error)

	// CompleteTask marks a task as successfully completed
	// Returns ErrTaskNotFound if the task doesn't exist
	CompleteTask(ctx context.Context, id string, result []byte) error

	// FailTask marks a task as failed
	// Returns ErrTaskNotFound if the task doesn't exist
	FailTask(ctx context.Context, id string, errMsg string) error

	// RetryTask schedules a failed task for retry
	// Returns ErrTaskNotFound if the task doesn't exist
	// Returns ErrMaxRetriesExceeded if the task has reached its retry limit
	RetryTask(ctx context.Context, id string, backoffSeconds int) error

	// Stats returns current queue statistics
	Stats(ctx context.Context) (Stats, error)

	// Shutdown gracefully shuts down the broker
	// It stops accepting new tasks and waits for in-progress tasks to complete
	Shutdown(ctx context.Context) error
}

// Stats represents queue statistics
type Stats struct {
	Pending   int       `json:"pending"`
	Running   int       `json:"running"`
	Completed int       `json:"completed"`
	Failed    int       `json:"failed"`
	Retrying  int       `json:"retrying"`
	Total     int       `json:"total"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Standard errors returned by broker implementations
var (
	ErrTaskNotFound       = NewError("task not found")
	ErrDuplicateTask      = NewError("duplicate task")
	ErrNoTaskAvailable    = NewError("no task available")
	ErrMaxRetriesExceeded = NewError("maximum retries exceeded")
	ErrInvalidState       = NewError("invalid task state")
	ErrBrokerClosed       = NewError("broker is closed")
)
