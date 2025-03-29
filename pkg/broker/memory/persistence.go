package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"go.uber.org/zap"
)

// PersistenceOptions configures the persistence behavior
type PersistenceOptions struct {
	// Directory to store persistence files
	Directory string

	// How often to save state (0 = disabled)
	CheckpointInterval time.Duration

	// Whether to load state on startup
	LoadOnStartup bool
}

// PersistenceManager handles saving and loading broker state
type PersistenceManager struct {
	options PersistenceOptions
	broker  *MemoryBroker
	logger  *zap.Logger
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewPersistenceManager creates a new persistence manager
func NewPersistenceManager(broker *MemoryBroker, options PersistenceOptions, logger *zap.Logger) *PersistenceManager {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &PersistenceManager{
		options: options,
		broker:  broker,
		logger:  logger,
		stopCh:  make(chan struct{}),
	}
}

// Start begins the persistence manager
func (p *PersistenceManager) Start() error {
	// Create directory if it doesn't exist
	if p.options.Directory != "" {
		if err := os.MkdirAll(p.options.Directory, 0755); err != nil {
			return err
		}
	}

	// Load state if configured
	if p.options.LoadOnStartup {
		if err := p.LoadState(); err != nil {
			p.logger.Warn("Failed to load state", zap.Error(err))
			// Continue even if load fails
		}
	}

	// Start checkpoint goroutine if interval > 0
	if p.options.CheckpointInterval > 0 {
		p.wg.Add(1)
		go p.checkpointLoop()
	}

	return nil
}

// Stop stops the persistence manager
func (p *PersistenceManager) Stop() {
	close(p.stopCh)
	p.wg.Wait()
}

// SaveState saves the current broker state to disk
func (p *PersistenceManager) SaveState() error {
	if p.options.Directory == "" {
		return nil // Persistence disabled
	}

	p.logger.Info("Saving broker state")

	// Create a snapshot of the current state
	snapshot := p.createSnapshot()

	// Marshal to JSON
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	// Write to file
	filename := filepath.Join(p.options.Directory, "broker-state.json")
	tempFile := filename + ".tmp"

	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return err
	}

	// Atomically replace the old file
	if err := os.Rename(tempFile, filename); err != nil {
		return err
	}

	p.logger.Info("Broker state saved successfully",
		zap.Int("tasks", len(snapshot.Tasks)),
		zap.String("file", filename))

	return nil
}

// LoadState loads the broker state from disk
func (p *PersistenceManager) LoadState() error {
	if p.options.Directory == "" {
		return nil // Persistence disabled
	}

	filename := filepath.Join(p.options.Directory, "broker-state.json")

	// Check if file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		p.logger.Info("No state file found, starting with empty state")
		return nil
	}

	// Read file
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	// Unmarshal JSON
	var snapshot brokerSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	// Restore state
	if err := p.restoreSnapshot(snapshot); err != nil {
		return err
	}

	p.logger.Info("Broker state loaded successfully",
		zap.Int("tasks", len(snapshot.Tasks)),
		zap.String("file", filename))

	return nil
}

// checkpointLoop periodically saves state
func (p *PersistenceManager) checkpointLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.options.CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := p.SaveState(); err != nil {
				p.logger.Error("Failed to save checkpoint", zap.Error(err))
			}
		case <-p.stopCh:
			// Final checkpoint on shutdown
			if err := p.SaveState(); err != nil {
				p.logger.Error("Failed to save final checkpoint", zap.Error(err))
			}
			return
		}
	}
}

// brokerSnapshot represents the serializable state of the broker
type brokerSnapshot struct {
	Tasks          []*task.Task      `json:"tasks"`
	IdempotencyMap map[string]string `json:"idempotency_map"`
	CreatedAt      time.Time         `json:"created_at"`
}

// createSnapshot creates a serializable snapshot of the broker state
func (p *PersistenceManager) createSnapshot() brokerSnapshot {
	p.broker.mu.RLock()
	defer p.broker.mu.RUnlock()

	tasks := make([]*task.Task, 0, len(p.broker.tasks))
	for _, t := range p.broker.tasks {
		tasks = append(tasks, t.Clone())
	}

	// Copy idempotency map
	idempotencyMap := make(map[string]string)
	for k, v := range p.broker.idempotencyMap {
		idempotencyMap[k] = v
	}

	return brokerSnapshot{
		Tasks:          tasks,
		IdempotencyMap: idempotencyMap,
		CreatedAt:      time.Now().UTC(),
	}
}

// restoreSnapshot restores broker state from a snapshot
func (p *PersistenceManager) restoreSnapshot(snapshot brokerSnapshot) error {
	p.broker.mu.Lock()
	defer p.broker.mu.Unlock()

	// Clear existing state
	p.broker.tasks = make(map[string]*task.Task)
	p.broker.idempotencyMap = make(map[string]string)

	// Restore tasks
	for _, t := range snapshot.Tasks {
		p.broker.tasks[t.GetID()] = t

		// Re-add pending tasks to the queue
		if t.GetState() == task.StatePending || t.GetState() == task.StateRetryable {
			var readyAt time.Time
			if nextRun := t.GetNextRunTime(); nextRun != nil {
				readyAt = *nextRun
			} else {
				readyAt = time.Now()
			}

			p.broker.pendingQueue.PushTask(t.GetID(), t.GetPriority(), readyAt)
		}
	}

	// Restore idempotency map
	for k, v := range snapshot.IdempotencyMap {
		p.broker.idempotencyMap[k] = v
	}

	return nil
}
