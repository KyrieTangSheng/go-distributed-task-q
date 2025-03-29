package task

import "time"

// Option is a function that configures a Task
type Option func(*Task)

// WithIdempotencyKey sets a custom idempotency key for the task.
// This allows clients to prevent duplicate task execution.
func WithIdempotencyKey(key string) Option {
	return func(t *Task) {
		t.IdempotencyKey = key
	}
}

// WithCorrelationID sets a custom correlation ID for the task.
// This is useful for tracing related tasks across the system.
func WithCorrelationID(id string) Option {
	return func(t *Task) {
		t.CorrelationID = id
	}
}

// WithParentTask sets this task as a child of another task.
// This creates a task hierarchy for complex workflows.
func WithParentTask(parentID string) Option {
	return func(t *Task) {
		t.ParentTaskID = parentID
	}
}

// WithPriority sets the processing priority of the task.
// Higher values indicate higher priority.
func WithPriority(priority int) Option {
	return func(t *Task) {
		t.Priority = priority
	}
}

// WithMaxRetries sets the maximum number of retry attempts.
func WithMaxRetries(maxRetries int) Option {
	return func(t *Task) {
		t.MaxRetries = maxRetries
	}
}

// WithTimeout sets the execution timeout in seconds.
func WithTimeout(timeoutSeconds int) Option {
	return func(t *Task) {
		t.Timeout = timeoutSeconds
	}
}

// WithMetadata adds key-value metadata to the task.
func WithMetadata(key, value string) Option {
	return func(t *Task) {
		if t.Metadata == nil {
			t.Metadata = make(map[string]string)
		}
		t.Metadata[key] = value
	}
}

// WithScheduledExecution sets the task to execute at a future time.
func WithScheduledExecution(executeAt time.Time) Option {
	return func(t *Task) {
		t.State = StateScheduled
		t.NextRetryAt = &executeAt
	}
}
