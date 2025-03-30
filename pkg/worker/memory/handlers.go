package memory

import (
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/worker"
	"go.uber.org/zap"
)

// RegisterHandler registers a handler for a specific task type
func (w *MemoryWorker) RegisterHandler(taskType string, handler worker.Handler) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check if worker is shutting down
	if w.shuttingDown {
		return worker.ErrWorkerStopped
	}

	// Check if handler already exists
	if _, exists := w.handlers[taskType]; exists {
		w.logger.Warn("Handler already registered for task type",
			zap.String("task_type", taskType))
		return worker.ErrHandlerAlreadyRegistered
	}

	// Register the handler
	w.handlers[taskType] = handler
	w.logger.Info("Handler registered for task type",
		zap.String("task_type", taskType))

	return nil
}

// getHandler returns the handler for a specific task type
func (w *MemoryWorker) getHandler(taskType string) (worker.Handler, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	handler, exists := w.handlers[taskType]
	if !exists {
		return nil, worker.ErrHandlerNotFound
	}

	return handler, nil
}
