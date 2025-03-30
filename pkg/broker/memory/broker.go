package memory

import (
	"sync"
	"time"

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
