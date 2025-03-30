package task

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrTaskLocked             = errors.New("task is locked by another operation")
	ErrTaskExpired            = errors.New("task has expired")
	ErrTaskRetriesExceeded    = errors.New("maximum retry attemptsexceeded")
)

// Task represents a unit of work in the queue system.
// It contains all necessary metadata for reliable processing.
type Task struct {
	// Concurrency control
	mu sync.RWMutex

	// Core Identification
	ID             string `json:"id"`
	IdempotencyKey string `json:"idempotency_key"`
	Type           string `json:"type"`

	// Payload
	Payload json.RawMessage `json:"payload"`

	// State Management
	State       State      `json:"state"`
	SubmittedAt time.Time  `json:"submitted_at"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`

	// Retry Management
	RetryCount   int        `json:"retry_count"`
	MaxRetries   int        `json:"max_retries"`
	NextRetryAt  *time.Time `json:"next_retry_at"`
	ErrorMessage string     `json:"error_message"`

	// Observability
	CorrelationID string `json:"correlation_id"`
	ParentTaskID  string `json:"parent_task_id"`

	// Execution Control
	Priority int `json:"priority"`
	Timeout  int `json:"timeout"`

	// Metadata
	Metadata map[string]string `json:"metadata"`

	// Dead Letter Queue
	UpdatedAt time.Time `json:"updated_at"`
}

// NewTask creates a new task with the given type and payload.
// It generates a unique ID and sets default values.
func NewTask(taskType string, payload json.RawMessage, opts ...Option) *Task {
	now := time.Now().UTC()

	task := &Task{
		ID:            uuid.New().String(),
		Type:          taskType,
		Payload:       payload,
		State:         StatePending,
		SubmittedAt:   now,
		RetryCount:    0,
		MaxRetries:    3, // Default retry count
		CorrelationID: uuid.New().String(),
		Priority:      1,   // Default priority
		Timeout:       300, // Default timeout (5 minutes)
		Metadata:      make(map[string]string),
	}

	// Apply any provided options
	for _, opt := range opts {
		opt(task)
	}

	// If no idempotency key was provided, use the task ID
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = task.ID
	}

	return task
}

// GetState returns the current state of the task in a thread-safe manner
func (t *Task) GetState() State {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.State
}

// GetID returns the task ID in a thread-safe manner
func (t *Task) GetID() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.ID
}

// GetIdempotencyKey returns the idempotency key in a thread-safe manner
func (t *Task) GetIdempotencyKey() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.IdempotencyKey
}

// GetType returns the task type in a thread-safe manner
func (t *Task) GetType() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Type
}

// GetPayload returns a copy of the task payload in a thread-safe manner
func (t *Task) GetPayload() json.RawMessage {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// Create a copy to avoid potential race conditions
	payloadCopy := make(json.RawMessage, len(t.Payload))
	copy(payloadCopy, t.Payload)
	return payloadCopy
}

// MarkStarted updates the task state to indicate processing has begun
func (t *Task) MarkStarted() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.State.CanTransitionTo(StateRunning) {
		return ErrInvalidStateTransition
	}

	now := time.Now().UTC()
	t.StartedAt = &now
	t.State = StateRunning
	return nil
}

// MarkCompleted updates the task state to indicate successful completion
func (t *Task) MarkCompleted() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.State.CanTransitionTo(StateCompleted) {
		return ErrInvalidStateTransition
	}

	now := time.Now().UTC()
	t.CompletedAt = &now
	t.State = StateCompleted
	return nil
}

// MarkFailed updates the task state to indicate a failure
func (t *Task) MarkFailed(errMsg string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.State.CanTransitionTo(StateFailed) {
		return ErrInvalidStateTransition
	}

	t.State = StateFailed
	t.ErrorMessage = errMsg
	return nil
}

// ShouldRetry determines if the task should be retried based on retry count and policy
func (t *Task) ShouldRetry() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.RetryCount < t.MaxRetries &&
		(t.State == StateFailed || t.State == StateRetryable)
}

// PrepareForRetry updates the task for a retry attempt
func (t *Task) PrepareForRetry(backoffSeconds int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.RetryCount >= t.MaxRetries {
		return ErrTaskRetriesExceeded
	}

	if t.State != StateFailed && t.State != StateRetryable && t.State != StateTimeout {
		return ErrInvalidStateTransition
	}

	t.RetryCount++
	t.State = StatePending

	nextRetry := time.Now().UTC().Add(time.Duration(backoffSeconds) * time.Second)
	t.NextRetryAt = &nextRetry
	return nil
}

// IsExpired checks if the task has exceeded its execution timeout
func (t *Task) IsExpired() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.StartedAt == nil || t.State != StateRunning {
		return false
	}

	timeout := time.Duration(t.Timeout) * time.Second
	return time.Since(*t.StartedAt) > timeout
}

// MarkExpired marks a task as timed out
func (t *Task) MarkExpired() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.State != StateRunning {
		return ErrInvalidStateTransition
	}

	if t.StartedAt == nil {
		return errors.New("cannot mark as expired: task has not started")
	}

	timeout := time.Duration(t.Timeout) * time.Second
	if time.Since(*t.StartedAt) <= timeout {
		return errors.New("cannot mark as expired: task has not exceeded timeout")
	}

	t.State = StateTimeout
	t.ErrorMessage = "Task execution timed out"
	return nil
}

// GetMetadata retrieves a metadata value by key in a thread-safe manner
func (t *Task) GetMetadata(key string) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	value, exists := t.Metadata[key]
	return value, exists
}

// SetMetadata sets a metadata value in a thread-safe manner
func (t *Task) SetMetadata(key, value string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}
	t.Metadata[key] = value
}

// Clone creates a deep copy of the task
func (t *Task) Clone() *Task {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Create a new task with the same basic properties
	clone := &Task{
		ID:             t.ID,
		IdempotencyKey: t.IdempotencyKey,
		Type:           t.Type,
		State:          t.State,
		SubmittedAt:    t.SubmittedAt,
		RetryCount:     t.RetryCount,
		MaxRetries:     t.MaxRetries,
		ErrorMessage:   t.ErrorMessage,
		CorrelationID:  t.CorrelationID,
		ParentTaskID:   t.ParentTaskID,
		Priority:       t.Priority,
		Timeout:        t.Timeout,
	}

	// Copy payload
	if t.Payload != nil {
		clone.Payload = make(json.RawMessage, len(t.Payload))
		copy(clone.Payload, t.Payload)
	}

	// Copy pointer fields
	if t.StartedAt != nil {
		startedAt := *t.StartedAt
		clone.StartedAt = &startedAt
	}

	if t.CompletedAt != nil {
		completedAt := *t.CompletedAt
		clone.CompletedAt = &completedAt
	}

	if t.NextRetryAt != nil {
		nextRetryAt := *t.NextRetryAt
		clone.NextRetryAt = &nextRetryAt
	}

	// Copy metadata map
	if t.Metadata != nil {
		clone.Metadata = make(map[string]string, len(t.Metadata))
		for k, v := range t.Metadata {
			clone.Metadata[k] = v
		}
	}

	return clone
}

// GetPriority returns the task priority in a thread-safe manner
func (t *Task) GetPriority() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Priority
}

// GetNextRunTime returns the next scheduled run time in a thread-safe manner
func (t *Task) GetNextRunTime() *time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.NextRetryAt
}

// GetStartedAt returns the task start time in a thread-safe manner
func (t *Task) GetStartedAt() *time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.StartedAt
}

// GetRetryCount returns the current retry count in a thread-safe manner
func (t *Task) GetRetryCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.RetryCount
}

// GetMaxRetries returns the maximum retry limit in a thread-safe manner
func (t *Task) GetMaxRetries() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.MaxRetries
}

// StoreResultInfo stores result information in the task metadata
func (t *Task) StoreResultInfo(resultInfo string) {
	t.SetMetadata("result_summary", resultInfo)
}

// MarkDead marks a task as dead (permanently failed)
func (t *Task) MarkDead(reason string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Only failed tasks can be marked as dead
	if t.State != StateFailed {
		return ErrInvalidStateTransition
	}

	t.State = StateDead
	t.ErrorMessage = reason
	t.UpdatedAt = time.Now().UTC()

	return nil
}

// ResetForRetry resets a dead task for retry
func (t *Task) ResetForRetry() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Only dead tasks can be reset
	if t.State != StateDead {
		return ErrInvalidStateTransition
	}

	t.State = StatePending
	t.ErrorMessage = ""
	t.UpdatedAt = time.Now().UTC()

	// Reset retry count if it was maxed out
	if t.RetryCount >= t.MaxRetries {
		t.RetryCount = 0
	}

	return nil
}

// GetTimeout returns the task timeout in seconds in a thread-safe manner
func (t *Task) GetTimeout() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Timeout
}
