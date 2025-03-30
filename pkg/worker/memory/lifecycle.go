package memory

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/worker"
	"go.uber.org/zap"
)

// Start begins task processing
func (w *MemoryWorker) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return worker.ErrWorkerAlreadyRunning
	}
	w.running = true
	w.mu.Unlock()

	w.logger.Info("Worker starting",
		zap.Int("max_concurrent", w.maxConcurrent),
		zap.Duration("poll_interval", w.pollInterval))

	// Start the main processing loop
	go w.processLoop(ctx)

	return nil
}

// Stop gracefully stops the worker
func (w *MemoryWorker) Stop(ctx context.Context) error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil // Already stopped
	}
	w.shuttingDown = true
	w.running = false
	w.mu.Unlock()

	w.logger.Info("Worker shutting down, waiting for tasks to complete")

	// Signal the processing loop to stop
	close(w.stopCh)

	// Wait for all tasks to complete with timeout
	done := make(chan struct{})
	go func() {
		w.taskWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.logger.Info("All tasks completed, worker stopped")
	case <-ctx.Done():
		w.logger.Warn("Shutdown timed out, some tasks may still be running")
	}

	return nil
}

// Stats returns current worker statistics
func (w *MemoryWorker) Stats(ctx context.Context) (worker.Stats, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.shuttingDown {
		return worker.Stats{}, worker.ErrWorkerStopped
	}

	procTimeStats := w.metrics.processingTime.Stats()

	return worker.Stats{
		ActiveTasks:         int(atomic.LoadInt32(&w.activeTasks)),
		CompletedTasks:      atomic.LoadUint64(&w.metrics.tasksCompleted),
		FailedTasks:         atomic.LoadUint64(&w.metrics.tasksFailed),
		AvgProcessingTimeMs: procTimeStats["avg"],
		MaxProcessingTimeMs: procTimeStats["max"],
		MinProcessingTimeMs: procTimeStats["min"],
		UpdatedAt:           time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// processLoop is the main processing loop
func (w *MemoryWorker) processLoop(ctx context.Context) {
	backoff := w.backoffMin

	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		default:
			// Check if we can process more tasks
			if atomic.LoadInt32(&w.activeTasks) >= int32(w.maxConcurrent) {
				time.Sleep(w.pollInterval)
				continue
			}

			// Try to get a task
			taskobj, err := w.broker.NextTask(ctx)
			if err != nil {
				if err == broker.ErrNoTaskAvailable {
					// No tasks available, apply backoff
					time.Sleep(backoff)
					// Increase backoff up to max
					backoff = min(backoff*2, w.backoffMax)
					continue
				}

				if err == broker.ErrBrokerClosed {
					w.logger.Info("Broker is closed, stopping worker")
					return
				}

				w.logger.Warn("Error getting next task", zap.Error(err))
				time.Sleep(w.pollInterval)
				continue
			}

			// Reset backoff when we get a task
			backoff = w.backoffMin

			// Process the task in a goroutine
			atomic.AddInt32(&w.activeTasks, 1)
			w.taskWg.Add(1)

			go func(t *task.Task) {
				defer w.taskWg.Done()
				defer atomic.AddInt32(&w.activeTasks, -1)

				w.processTask(ctx, t)
			}(taskobj)
		}
	}
}
