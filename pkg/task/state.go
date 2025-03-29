package task

// State represents the current state of a task
type State string

const (
	// Core States
	StatePending   State = "pending"   // Task is waiting to be processed
	StateRunning   State = "running"   // Task is currently being processed
	StateCompleted State = "completed" // Task has been successfully processed
	StateFailed    State = "failed"    // Task processing has failed

	// Advanced States
	StateRetryable State = "retryable" // Task failed but can be retried
	StateScheduled State = "scheduled" // Task is scheduled for future execution
	StateCancelled State = "cancelled" // Task was cancelled before completion
	StateTimeout   State = "timeout"   // Task exceeded its execution time limit
	StateDead      State = "dead"      // Task failed and exceeded retry limits
)

// IsTerminal returns true if the state represents a terminal state
// where no further processing will occur.
func (s State) IsTerminal() bool {
	return s == StateCompleted ||
		s == StateCancelled ||
		s == StateDead
}

// IsActive returns true if the task is currently being processed
// or waiting to be processed.
func (s State) IsActive() bool {
	return s == StatePending ||
		s == StateRunning ||
		s == StateScheduled ||
		s == StateRetryable
}

// ValidStateTransitions maps states to their valid next states
var ValidStateTransitions = map[State][]State{
	StatePending:   {StateRunning, StateCancelled},
	StateRunning:   {StateCompleted, StateFailed, StateTimeout},
	StateCompleted: {}, // Terminal state
	StateFailed:    {StateRetryable, StateDead},
	StateRetryable: {StatePending, StateDead},
	StateScheduled: {StatePending, StateCancelled},
	StateCancelled: {}, // Terminal state
	StateTimeout:   {StateRetryable, StateDead},
	StateDead:      {}, // Terminal state
}

// CanTransitionTo checks if the current state can transition to the target state
func (s State) CanTransitionTo(target State) bool {
	validTargets, exists := ValidStateTransitions[s]
	if !exists {
		return false
	}

	for _, validTarget := range validTargets {
		if validTarget == target {
			return true
		}
	}

	return false
}
