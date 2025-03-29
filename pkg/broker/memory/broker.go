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
}

// Options configures the memory broker
type Options struct {
	MaxConcurrent int
	Logger        *zap.Logger
}

// NewBroker creates a new in-memory broker
func NewBroker(opts Options) *MemoryBroker {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}

	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = 100 // Default concurrent tasks
	}

	return &MemoryBroker{
		tasks:          make(map[string]*task.Task),
		idempotencyMap: make(map[string]string),
		pendingQueue:   newTaskQueue(),
		maxConcurrent:  opts.MaxConcurrent,
		logger:         opts.Logger,
		metrics:        newMetrics(),
		startTime:      time.Now().UTC(),
	}
}
