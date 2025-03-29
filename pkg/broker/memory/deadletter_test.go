package memory

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/broker"
	"github.com/KyrieTangSheng/go-distributed-task-q/pkg/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDeadLetterQueue(t *testing.T) {
	// Setup
	b := NewBroker(Options{Logger: zap.NewNop()})
	ctx := context.Background()

	// Create a task with max retries = 0
	payload := json.RawMessage(`{"test":"data"}`)
	task1 := task.NewTask("test-type", payload, task.WithMaxRetries(0))

	// Submit the task
	err := b.SubmitTask(ctx, task1)
	require.NoError(t, err)

	// Get the task
	next, err := b.NextTask(ctx)
	require.NoError(t, err)

	// Fail the task - should go to dead letter queue
	err = b.FailTask(ctx, next.GetID(), "test failure")
	require.NoError(t, err)

	// Get dead letter tasks
	deadLetterTasks, err := b.GetDeadLetterTasks(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, len(deadLetterTasks))
	assert.Equal(t, next.GetID(), deadLetterTasks[0].GetID())
	assert.Equal(t, task.StateDead, deadLetterTasks[0].GetState())

	// Get specific dead letter task
	deadTask, err := b.GetDeadLetterTask(ctx, next.GetID())
	require.NoError(t, err)
	assert.Equal(t, next.GetID(), deadTask.GetID())
	assert.Equal(t, task.StateDead, deadTask.GetState())

	// Retry the dead letter task
	err = b.RetryDeadLetterTask(ctx, next.GetID())
	require.NoError(t, err)

	// Verify it's back in the pending queue
	retriedTask, err := b.NextTask(ctx)
	require.NoError(t, err)
	assert.Equal(t, next.GetID(), retriedTask.GetID())
	assert.Equal(t, task.StateRunning, retriedTask.GetState())

	// Fail it again
	err = b.FailTask(ctx, retriedTask.GetID(), "test failure again")
	require.NoError(t, err)

	// Delete from dead letter queue
	err = b.DeleteDeadLetterTask(ctx, retriedTask.GetID())
	require.NoError(t, err)

	// Verify it's gone
	_, err = b.GetDeadLetterTask(ctx, retriedTask.GetID())
	assert.Equal(t, broker.ErrTaskNotFound, err)
}
