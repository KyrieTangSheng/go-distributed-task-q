# Worker Service

## Overview

The Worker Service is responsible for executing tasks retrieved from the broker. It:

- Connects to a broker to retrieve tasks
- Registers handlers for different task types
- Executes tasks with the appropriate handler
- Reports results back to the broker
- Manages concurrency and resource limits
- Provides metrics and observability
- Supports graceful shutdown

## Interface

The Worker interface defines the contract that all worker implementations must follow:
```go
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
```

## Task Handlers

Task handlers are functions that process a specific task type:
```go
type Handler func(ctx context.Context, task *task.Task) ([]byte, error)
```

Handlers should:
- Process the task payload
- Return a result (can be empty)
- Return an error if processing fails
- Use `worker.NewRetryableError()` to indicate retryable failures

### Handler Best Practices

1. **Idempotency**: Handlers should be idempotent whenever possible, as tasks may be executed multiple times.
2. **Timeouts**: Use the context deadline to ensure tasks don't run indefinitely.
3. **Error Handling**: Distinguish between retryable and non-retryable errors.
4. **Resource Management**: Close resources (files, connections) properly.
5. **Logging**: Include task ID in logs for traceability.

## Error Handling

The worker provides a special error type for retryable failures:

```go
// Create a retryable error
err := worker.NewRetryableError(errors.New("temporary failure"))

// Check if an error is retryable
isRetryable := worker.IsRetryable(err)
```

When a handler returns a retryable error:
1. The worker marks the task for retry with exponential backoff
2. The broker reschedules the task after the backoff period
3. The task will be retried until it succeeds or exceeds its retry limit

## Middleware Support

The worker supports middleware for cross-cutting concerns:

```go
// Create middleware
loggingMiddleware := middleware.Logging(logger)
recoveryMiddleware := middleware.Recovery(logger)

// Chain middleware
mw := middleware.Chain(loggingMiddleware, recoveryMiddleware)

// Register handler with middleware
worker.RegisterHandler("email", mw(handleEmailTask))
```

Built-in middleware includes:
- **Logging**: Logs task execution details
- **Recovery**: Recovers from panics in task handlers
- **Metrics**: Collects detailed performance metrics

## In-Memory Implementation

The `memory.MemoryWorker` provides an in-memory implementation suitable for:
- Development environments
- Testing
- Small-scale production deployments

### Features

- Thread-safe operations
- Concurrent task execution with limits
- Automatic backoff when no tasks are available
- Exponential retry with jitter for failed tasks
- Comprehensive metrics
- Structured logging
- Graceful shutdown
- Middleware support for cross-cutting concerns

### Configuration
```go
// Basic configuration
opts := memory.Options{
    MaxConcurrent: 10,       // Maximum concurrent tasks
    PollInterval:  100 * time.Millisecond,
    BackoffMin:    100 * time.Millisecond,
    BackoffMax:    5 * time.Second,
    Logger:        logger,
}

worker := memory.NewWorker(broker, opts)
```

## Worker Lifecycle

1. **Initialization**: Create a worker and register handlers
   ```go
   worker := memory.NewWorker(broker, opts)
   worker.RegisterHandler("email", handleEmailTask)
   worker.RegisterHandler("notification", handleNotificationTask)
   ```

2. **Starting**: Begin processing tasks
   ```go
   err := worker.Start(ctx)
   ```

3. **Processing**: The worker automatically polls for tasks and executes them

4. **Shutdown**: Gracefully stop the worker
   ```go
   ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
   defer cancel()
   err := worker.Stop(ctx)
   ```

## Monitoring and Metrics

The worker provides statistics through the `Stats()` method:
```go
stats, err := worker.Stats(ctx)
fmt.Printf("Active tasks: %d\n", stats.ActiveTasks)
fmt.Printf("Completed tasks: %d\n", stats.CompletedTasks)
fmt.Printf("Failed tasks: %d\n", stats.FailedTasks)
fmt.Printf("Avg processing time: %dms\n", stats.AvgProcessingTimeMs)
```

Available metrics include:
- Active tasks count
- Completed tasks count
- Failed tasks count
- Processing time statistics (min, max, avg)

## Example Usage

```go
// Create a broker
broker := memory.NewBroker(brokerOpts)

// Create a worker
worker := memory.NewWorker(broker, workerOpts)

// Register handlers
worker.RegisterHandler("email", handleEmailTask)
worker.RegisterHandler("notification", handleNotificationTask)

// Start the worker
if err := worker.Start(ctx); err != nil {
    log.Fatalf("Failed to start worker: %v", err)
}

// Wait for interrupt signal
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
<-sigCh

// Graceful shutdown
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := worker.Stop(shutdownCtx); err != nil {
    log.Errorf("Error during worker shutdown: %v", err)
}
```

## Performance Considerations

1. **Concurrency**: Set `MaxConcurrent` based on available resources and task characteristics
2. **Polling Interval**: Balance responsiveness with broker load
3. **Backoff Strategy**: Adjust based on expected task availability patterns
4. **Handler Efficiency**: Optimize handlers for the most common task types
5. **Resource Management**: Ensure handlers properly release resources

## Integration with Broker

The worker relies on the broker for:
1. Retrieving tasks (`NextTask`)
2. Reporting completion (`CompleteTask`)
3. Reporting failures (`FailTask`)
4. Requesting retries (`RetryTask`)

Ensure the broker implementation is compatible with the worker's requirements.



