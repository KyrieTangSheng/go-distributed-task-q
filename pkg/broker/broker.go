package broker

import (
	"context"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
)

// Broker defines the interface for task queue brokers.
// It handles task submission, distribution, and lifecycle management.
type Broker interface {
	// Task submission and retrieval
	SubmitTask(ctx context.Context, task *task.Task) error
	GetTask(ctx context.Context, id string) (*task.Task, error)

	// Task distribution
	NextTask(ctx context.Context) (*task.Task, error)

	// Task lifecycle management
	CompleteTask(ctx context.Context, id string, result []byte) error
	FailTask(ctx context.Context, id string, errMsg string) error
	RetryTask(ctx context.Context, id string, backoffSeconds int) error

	// Dead letter queue management
	GetDeadLetterTasks(ctx context.Context) ([]*task.Task, error)
	GetDeadLetterTask(ctx context.Context, id string) (*task.Task, error)
	RetryDeadLetterTask(ctx context.Context, id string) error
	DeleteDeadLetterTask(ctx context.Context, id string) error

	// Queue management
	Stats(ctx context.Context) (Stats, error)
	Shutdown(ctx context.Context) error
}

// Stats represents broker statistics
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
