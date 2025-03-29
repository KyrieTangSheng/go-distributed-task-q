package memory

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// Shutdown gracefully shuts down the broker
func (b *MemoryBroker) Shutdown(ctx context.Context) error {
	b.logger.Info("Broker shutdown initiated")

	// Mark as shutting down to prevent new tasks
	b.mu.Lock()
	b.shuttingDown = true
	b.mu.Unlock()

	// Wait for running tasks to complete or context to cancel
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.logger.Warn("Broker shutdown interrupted", zap.Error(ctx.Err()))

			// Stop persistence manager if it exists
			if b.persistence != nil {
				b.persistence.Stop()
			}

			return ctx.Err()
		case <-ticker.C:
			// Check if there are any running tasks
			stats, _ := b.Stats(ctx)
			if stats.Running == 0 {
				// Final persistence checkpoint
				if b.persistence != nil {
					if err := b.persistence.SaveState(); err != nil {
						b.logger.Warn("Failed to save final state during shutdown",
							zap.Error(err))
					}
					b.persistence.Stop()
				}

				b.logger.Info("Broker shutdown complete",
					zap.Int("total_tasks", stats.Total),
					zap.Int("completed_tasks", stats.Completed),
					zap.Int("failed_tasks", stats.Failed))
				return nil
			}
			b.logger.Info("Waiting for running tasks to complete",
				zap.Int("running_tasks", stats.Running))
		}
	}
}
