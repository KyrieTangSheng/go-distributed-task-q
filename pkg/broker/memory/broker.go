package memory

import (
	"context"
	"sync"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"go.uber.org/zap"
)

// MemoryBroker implements the broker.Broker interface with in-memory storage
type MemoryBroker struct {
	// Concurrency control
	mu           sync.RWMutex
	shuttingDown bool
	startTime    time.Time

	// Storage
	tasks          map[string]*task.Task
	idempotencyMap map[string]string // Maps idempotency keys to task IDs

	// Task queues (we'll implement these next)
	pendingQueue *taskQueue

	// Configuration
	maxConcurrent int

	// Observability
	logger  *zap.Logger
	metrics *Metrics

	// Dead Letter Queue
	deadLetterQueue *DeadLetterQueue

	// Persistence
	persistence *PersistenceManager
}

// Options configures the memory broker
type Options struct {
	// Maximum number of concurrent tasks (0 = unlimited)
	MaxConcurrent int
	// Logger instance
	Logger *zap.Logger
	// Persistence options
	PersistenceOptions *PersistenceOptions
}

// NewBroker creates a new in-memory broker
func NewBroker(opts Options) *MemoryBroker {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}

	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = 100 // Default concurrent tasks
	}

	broker := &MemoryBroker{
		tasks:           make(map[string]*task.Task),
		idempotencyMap:  make(map[string]string),
		pendingQueue:    newTaskQueue(),
		maxConcurrent:   opts.MaxConcurrent,
		logger:          opts.Logger,
		metrics:         newMetrics(),
		startTime:       time.Now().UTC(),
		deadLetterQueue: NewDeadLetterQueue(),
	}

	// Initialize persistence if configured
	if opts.PersistenceOptions != nil {
		broker.persistence = NewPersistenceManager(broker, *opts.PersistenceOptions, opts.Logger)
		if err := broker.persistence.Start(); err != nil {
			opts.Logger.Error("Failed to start persistence manager", zap.Error(err))
		}
	}

	return broker
}

// GetDeadLetterTasks returns all tasks in the dead letter queue
func (b *MemoryBroker) GetDeadLetterTasks(ctx context.Context) ([]*task.Task, error) {
	if b.isShuttingDown() {
		return nil, broker.ErrBrokerClosed
	}

	return b.deadLetterQueue.List(), nil
}

// GetDeadLetterTask retrieves a specific task from the dead letter queue
func (b *MemoryBroker) GetDeadLetterTask(ctx context.Context, id string) (*task.Task, error) {
	if b.isShuttingDown() {
		return nil, broker.ErrBrokerClosed
	}

	t, exists := b.deadLetterQueue.Get(id)
	if !exists {
		return nil, broker.ErrTaskNotFound
	}

	return t, nil
}

// RetryDeadLetterTask moves a task from the dead letter queue back to the pending queue
func (b *MemoryBroker) RetryDeadLetterTask(ctx context.Context, id string) error {
	if b.isShuttingDown() {
		return broker.ErrBrokerClosed
	}

	// Get the task from the dead letter queue
	t, exists := b.deadLetterQueue.Get(id)
	if !exists {
		return broker.ErrTaskNotFound
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove from dead letter queue
	b.deadLetterQueue.Remove(id)

	// Reset the task for retry
	t.ResetForRetry()

	// Store in main task map
	b.tasks[id] = t

	// Add to pending queue
	b.pendingQueue.PushTask(id, t.GetPriority(), time.Now())

	b.logger.Info("Task moved from dead letter queue to pending queue",
		zap.String("task_id", id),
		zap.String("task_type", t.GetType()))

	return nil
}

// DeleteDeadLetterTask permanently removes a task from the dead letter queue
func (b *MemoryBroker) DeleteDeadLetterTask(ctx context.Context, id string) error {
	if b.isShuttingDown() {
		return broker.ErrBrokerClosed
	}

	if !b.deadLetterQueue.Remove(id) {
		return broker.ErrTaskNotFound
	}

	b.logger.Info("Task permanently deleted from dead letter queue",
		zap.String("task_id", id))

	return nil
}
