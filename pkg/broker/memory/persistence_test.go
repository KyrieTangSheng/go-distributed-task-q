package memory

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPersistence(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "broker-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Setup persistence options
	persistOpts := PersistenceOptions{
		Directory:          tempDir,
		CheckpointInterval: 1 * time.Second,
		LoadOnStartup:      true,
	}

	// Create broker with persistence
	b := NewBroker(Options{
		Logger:             zap.NewNop(),
		PersistenceOptions: &persistOpts,
	})
	ctx := context.Background()

	// Create and submit tasks
	payload := json.RawMessage(`{"test":"data"}`)
	task1 := task.NewTask("test-type-1", payload)
	task2 := task.NewTask("test-type-2", payload)
	task3 := task.NewTask("test-type-3", payload)

	err = b.SubmitTask(ctx, task1)
	require.NoError(t, err)
	err = b.SubmitTask(ctx, task2)
	require.NoError(t, err)
	err = b.SubmitTask(ctx, task3)
	require.NoError(t, err)

	// Complete one task
	next, err := b.NextTask(ctx)
	require.NoError(t, err)
	err = b.CompleteTask(ctx, next.GetID(), []byte(`{"result":"success"}`))
	require.NoError(t, err)

	// Fail one task
	next, err = b.NextTask(ctx)
	require.NoError(t, err)
	err = b.FailTask(ctx, next.GetID(), "test failure")
	require.NoError(t, err)

	// Force a checkpoint
	err = b.persistence.SaveState()
	require.NoError(t, err)

	// Verify the checkpoint file exists
	_, err = os.Stat(filepath.Join(tempDir, "broker-state.json"))
	require.NoError(t, err)

	// Create a new broker that will load the state
	b2 := NewBroker(Options{
		Logger:             zap.NewNop(),
		PersistenceOptions: &persistOpts,
	})

	// Verify the state was loaded
	stats, err := b2.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, stats.Total)
	assert.Equal(t, 1, stats.Completed)
	assert.Equal(t, 1, stats.Failed)
	assert.Equal(t, 1, stats.Pending)

	// Shutdown the brokers
	err = b.Shutdown(ctx)
	require.NoError(t, err)
	err = b2.Shutdown(ctx)
	require.NoError(t, err)
}
