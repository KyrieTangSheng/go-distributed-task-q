# Broker Service

## Overview
The Broker is the central component of GoTaskQ, responsible for:
- Accepting task submissions
- Storing tasks
- Distributing tasks to workers
- Tracking task state
- Providing observability

## Interface
The Broker interface defines the contract that all broker implementations must follow:

```go
type Broker interface {
// Task submission and retrieval
SubmitTask(ctx context.Context, task task.Task) error
GetTask(ctx context.Context, id string) (task.Task, error)
// Task distribution
NextTask(ctx context.Context) (task.Task, error)
// Task lifecycle management
CompleteTask(ctx context.Context, id string, result []byte) error
FailTask(ctx context.Context, id string, errMsg string) error
RetryTask(ctx context.Context, id string, backoffSeconds int) error
// Queue management
Stats(ctx context.Context) (Stats, error)
Shutdown(ctx context.Context) error
}
```

## In-Memory Implementation
The `memory.MemoryBroker` provides an in-memory implementation suitable for:
- Development environments
- Testing
- Small-scale production deployments

### Features
- Thread-safe operations
- Priority-based task scheduling
- Delayed task execution
- Idempotency guarantees
- Comprehensive metrics
- Structured logging
- Graceful shutdown

### Configuration

```go
opts := memory.Options{
MaxConcurrent: 100, // Maximum concurrent tasks
Logger: logger, // Zap logger instance
}
broker := memory.NewBroker(opts)
```

## Usage Examples

### Submitting a Task

```go
payload := json.RawMessage({"user_id": 123, "action": "send_email"})
task := task.NewTask("email_notification", payload)
ctx := context.Background()
err := broker.SubmitTask(ctx, task)
if err != nil {
log.Printf("Failed to submit task: %v", err)
return
}
```

### Processing Tasks (Worker Side)
```go
ctx := context.Background()
// Get next available task
task, err := broker.NextTask(ctx)
if err != nil {
if errors.Is(err, broker.ErrNoTaskAvailable) {
// No tasks available, try again later
return
}
log.Printf("Error getting next task: %v", err)
return
}
// Process the task
result, err := processTask(task)
if err != nil {
// Handle failure
failErr := broker.FailTask(ctx, task.GetID(), err.Error())
if failErr != nil {
log.Printf("Error marking task as failed: %v", failErr)
}
return
}
// Mark as completed
completeErr := broker.CompleteTask(ctx, task.GetID(), result)
if completeErr != nil {
log.Printf("Error marking task as completed: %v", completeErr)
}
```

### Graceful Shutdown
```go
// Create a context with timeout
ctx, cancel := context.WithTimeout(context.Background(), 30time.Second)
defer cancel()
// Shutdown the broker
err := broker.Shutdown(ctx)
if err != nil {
log.Printf("Error during broker shutdown: %v", err)
}
```


## Error Handling
The broker returns standardized errors:
- `ErrTaskNotFound`: When a requested task doesn't exist
- `ErrDuplicateTask`: When submitting a task with a duplicate idempotency key
- `ErrNoTaskAvailable`: When no tasks are available for processing
- `ErrMaxRetriesExceeded`: When a task has reached its retry limit
- `ErrInvalidState`: When attempting an invalid state transition
- `ErrBrokerClosed`: When the broker is shutting down

## Metrics
The broker collects the following metrics:
- Tasks submitted, completed, failed, and retried
- Queue depth
- Processing time (min, max, avg)

## Best Practices
1. **Always use context** for timeout and cancellation
2. **Handle ErrNoTaskAvailable** with appropriate backoff
3. **Implement proper shutdown** in your application
4. **Monitor queue metrics** for backpressure
5. **Use idempotency keys** for critical operations