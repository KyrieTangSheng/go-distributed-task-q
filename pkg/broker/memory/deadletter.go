package memory

import (
	"context"
	"sync"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"go.uber.org/zap"
)

// DeadLetterQueue stores tasks that have permanently failed
type DeadLetterQueue struct {
	mu    sync.RWMutex
	tasks map[string]*task.Task
}

// NewDeadLetterQueue creates a new dead letter queue
func NewDeadLetterQueue() *DeadLetterQueue {
	return &DeadLetterQueue{
		tasks: make(map[string]*task.Task),
	}
}

// MoveToDeadLetter moves a failed task to the dead letter queue
func (b *MemoryBroker) MoveToDeadLetter(ctx context.Context, id string, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Find the task
	t, exists := b.tasks[id]
	if !exists {
		return broker.ErrTaskNotFound
	}

	// Mark as dead
	err := t.MarkDead(reason)
	if err != nil {
		b.logger.Warn("Failed to mark task as dead",
			zap.String("task_id", id),
			zap.Error(err))
		return err
	}

	// Add to dead letter queue
	b.deadLetterQueue.Add(t.Clone())

	b.logger.Info("Task moved to dead letter queue",
		zap.String("task_id", id),
		zap.String("reason", reason))

	return nil
}

// Add adds a task to the dead letter queue
func (q *DeadLetterQueue) Add(t *task.Task) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks[t.GetID()] = t
}

// Get retrieves a task from the dead letter queue
func (q *DeadLetterQueue) Get(id string) (*task.Task, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	t, exists := q.tasks[id]
	if !exists {
		return nil, false
	}
	return t.Clone(), true
}

// List returns all tasks in the dead letter queue
func (q *DeadLetterQueue) List() []*task.Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	result := make([]*task.Task, 0, len(q.tasks))
	for _, t := range q.tasks {
		result = append(result, t.Clone())
	}
	return result
}

// Remove removes a task from the dead letter queue
func (q *DeadLetterQueue) Remove(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	_, exists := q.tasks[id]
	if !exists {
		return false
	}

	delete(q.tasks, id)
	return true
}
