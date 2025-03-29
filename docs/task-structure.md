# Task Structure Documentation

## Overview

The Task structure is the fundamental unit of work in GoTaskQ. It contains all the necessary information to process a piece of work reliably, track its progress, and ensure proper handling of failures.

## Design Principles

The Task structure was designed with the following principles in mind:

1. **Reliability**: Tasks include idempotency keys to prevent duplicate processing and support for retry mechanisms.
2. **Observability**: Comprehensive metadata, timestamps, and correlation IDs enable tracking and debugging.
3. **Fault Tolerance**: Detailed state management and error handling support graceful recovery from failures.
4. **Usability**: A flexible options pattern allows for customization while maintaining sensible defaults.
5. **Concurrency Safety**: All operations are thread-safe, allowing safe access from multiple goroutines.

## Core Components

### Identification
- **ID**: A unique identifier generated for each task
- **IdempotencyKey**: Client-provided key to prevent duplicate task creation
- **Type**: Determines which handler should process the task

### State Management
- **State**: Current position in the task lifecycle (pending, running, completed, etc.)
- **Timestamps**: When the task was submitted, started, and completed
- **RetryCount/MaxRetries**: Tracks retry attempts and limits

### Observability
- **CorrelationID**: Links related tasks and operations for tracing
- **ErrorMessage**: Captures failure details
- **Metadata**: Arbitrary key-value pairs for additional context

## Concurrency Safety

The Task structure is designed to be safely accessed from multiple goroutines:

- All state-changing methods are protected by a mutex
- Read operations use a read-lock for better performance
- The Clone() method provides a deep copy for safe manipulation

## State Machine

Tasks follow a well-defined state machine: 
┌─────────────┐
│ Pending │
└──────┬──────┘
│
▼
┌─────────────┐
│ Running │
└──────┬──────┘
│
┌─────────────┴─────────────┐
│ │
▼ ▼
┌─────────────┐ ┌─────────────┐
│ Completed │ │ Failed │
└─────────────┘ └──────┬──────┘
│
┌───────────┴───────────┐
│ │
▼ ▼
┌─────────────┐ ┌─────────────┐
│ Retryable │ │ Dead │
└──────┬──────┘ └─────────────┘
│
▼
┌─────────────┐
│ Pending │
└─────────────┘

## Error Handling

The Task implementation includes robust error handling:

- Invalid state transitions return appropriate errors
- Retry limits are enforced with clear error messages
- Timeout detection and handling is built-in
- All errors are properly typed for programmatic handling

## Usage Examples

### Creating a Basic Task
```go
payload := json.RawMessage({"user_id": 123, "action": "send_email"})
task := task.NewTask("email_notification", payload)
```

### Creating a Task with Options
```go
payload := json.RawMessage({"order_id": "ORD-12345", "amount": 99.99})
task := task.NewTask(
"process_payment",
payload,
task.WithIdempotencyKey("order-payment-ORD-12345"),
task.WithPriority(5),
task.WithMaxRetries(5),
task.WithCorrelationID("user-checkout-session-abc123"),
task.WithMetadata("customer_tier", "premium")
)
```

### Thread-Safe Task Manipulation
```go
// Start the task
if err := task.MarkStarted(); err != nil {
log.Printf("Failed to start task: %v", err)
return
}
// Process the task...
// Complete the task
if err := task.MarkCompleted(); err != nil {
log.Printf("Failed to complete task: %v", err)
return
}
```

### Handling Task Failures
```go
// Start the task
if err := task.MarkStarted(); err != nil {
log.Printf("Failed to start task: %v", err)
return
}
// Process the task...
err := processTask(task)
if err != nil {
// Mark as failed
if markErr := task.MarkFailed(err.Error()); markErr != nil {
log.Printf("Failed to mark task as failed: %v", markErr)
return
}
// Check if we should retry
if task.ShouldRetry() {
// Calculate backoff (e.g., exponential)
backoff := calculateBackoff(task.RetryCount)
if retryErr := task.PrepareForRetry(backoff); retryErr != nil {
log.Printf("Failed to prepare task for retry: %v", retryErr)
return
}
log.Printf("Task scheduled for retry in %d seconds", backoff)
}
}
```

## Best Practices

1. **Always provide idempotency keys** for business operations to prevent duplicate processing
2. **Use correlation IDs** to track related tasks across system boundaries
3. **Set appropriate timeouts** based on expected execution duration
4. **Include relevant metadata** to aid in debugging and monitoring
5. **Consider retry limits** based on the nature of the task
6. **Handle all errors** returned by task methods
7. **Use Clone()** when you need to modify a task