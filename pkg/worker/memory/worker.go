package memory

import (
	"sync"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/worker"
	"go.uber.org/zap"
)

// MemoryWorker implements the worker.Worker interface
type MemoryWorker struct {
	// Broker to pull tasks from
	broker broker.Broker

	// Concurrency control
	mu           sync.RWMutex
	shuttingDown bool
	startTime    time.Time

	// Registered task handlers
	handlers map[string]worker.Handler

	// Concurrency control
	maxConcurrent int
	activeTasks   int32
	taskWg        sync.WaitGroup

	// Worker state
	running bool
	stopCh  chan struct{}

	// Metrics
	metrics *metrics

	// Polling configuration
	pollInterval time.Duration
	backoffMin   time.Duration
	backoffMax   time.Duration

	// Observability
	logger *zap.Logger
}

// Options configures the memory worker
type Options struct {
	// Maximum number of concurrent tasks (0 = unlimited)
	MaxConcurrent int

	// Polling interval when tasks are available
	PollInterval time.Duration

	// Backoff configuration for when no tasks are available
	BackoffMin time.Duration
	BackoffMax time.Duration

	// Logger instance
	Logger *zap.Logger
}

// NewWorker creates a new in-memory worker
func NewWorker(b broker.Broker, opts Options) *MemoryWorker {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}

	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = 10 // Default concurrent tasks
	}

	if opts.PollInterval <= 0 {
		opts.PollInterval = 100 * time.Millisecond
	}

	if opts.BackoffMin <= 0 {
		opts.BackoffMin = 100 * time.Millisecond
	}

	if opts.BackoffMax <= 0 {
		opts.BackoffMax = 5 * time.Second
	}

	return &MemoryWorker{
		broker:        b,
		handlers:      make(map[string]worker.Handler),
		maxConcurrent: opts.MaxConcurrent,
		pollInterval:  opts.PollInterval,
		backoffMin:    opts.BackoffMin,
		backoffMax:    opts.BackoffMax,
		logger:        opts.Logger,
		stopCh:        make(chan struct{}),
		metrics:       newMetrics(),
		startTime:     time.Now().UTC(),
	}
}

// isShuttingDown checks if the worker is shutting down
func (w *MemoryWorker) isShuttingDown() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.shuttingDown
}
