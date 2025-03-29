package task

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestNewTask(t *testing.T) {
	payload := json.RawMessage(`{"test":"data"}`)
	task := NewTask("test_task", payload)

	if task.GetID() == "" {
		t.Error("Task ID should not be empty")
	}

	if task.GetType() != "test_task" {
		t.Errorf("Task type should be 'test_task', got '%s'", task.GetType())
	}

	if task.GetState() != StatePending {
		t.Errorf("Initial task state should be 'pending', got '%s'", task.GetState())
	}

	if task.GetIdempotencyKey() == "" {
		t.Error("Idempotency key should not be empty")
	}

	if task.CorrelationID == "" {
		t.Error("Correlation ID should not be empty")
	}
}

func TestTaskStateTransitions(t *testing.T) {
	payload := json.RawMessage(`{"test":"data"}`)
	task := NewTask("test_task", payload)

	// Test starting a task
	err := task.MarkStarted()
	if err != nil {
		t.Errorf("Failed to mark task as started: %v", err)
	}

	if task.GetState() != StateRunning {
		t.Errorf("Task state should be 'running', got '%s'", task.GetState())
	}

	if task.StartedAt == nil {
		t.Error("StartedAt should be set")
	}

	// Test completing a task
	err = task.MarkCompleted()
	if err != nil {
		t.Errorf("Failed to mark task as completed: %v", err)
	}

	if task.GetState() != StateCompleted {
		t.Errorf("Task state should be 'completed', got '%s'", task.GetState())
	}

	if task.CompletedAt == nil {
		t.Error("CompletedAt should be set")
	}

	// Create a new task for failure test
	task = NewTask("test_task", payload)
	err = task.MarkStarted()
	if err != nil {
		t.Errorf("Failed to mark task as started: %v", err)
	}

	err = task.MarkFailed("test error")
	if err != nil {
		t.Errorf("Failed to mark task as failed: %v", err)
	}

	if task.GetState() != StateFailed {
		t.Errorf("Task state should be 'failed', got '%s'", task.GetState())
	}

	if task.ErrorMessage != "test error" {
		t.Errorf("Error message should be 'test error', got '%s'", task.ErrorMessage)
	}
}

func TestInvalidStateTransitions(t *testing.T) {
	payload := json.RawMessage(`{"test":"data"}`)
	task := NewTask("test_task", payload)

	// Cannot complete a task that hasn't started
	err := task.MarkCompleted()
	if err == nil {
		t.Error("Should not be able to complete a task that hasn't started")
	}

	// Cannot fail a task that hasn't started
	err = task.MarkFailed("test error")
	if err == nil {
		t.Error("Should not be able to fail a task that hasn't started")
	}

	// Start the task
	err = task.MarkStarted()
	if err != nil {
		t.Errorf("Failed to mark task as started: %v", err)
	}

	// Complete the task
	err = task.MarkCompleted()
	if err != nil {
		t.Errorf("Failed to mark task as completed: %v", err)
	}

	// Cannot start a completed task
	err = task.MarkStarted()
	if err == nil {
		t.Error("Should not be able to start a completed task")
	}
}

func TestTaskRetry(t *testing.T) {
	payload := json.RawMessage(`{"test":"data"}`)
	task := NewTask("test_task", payload, WithMaxRetries(3))

	err := task.MarkStarted()
	if err != nil {
		t.Errorf("Failed to mark task as started: %v", err)
	}

	err = task.MarkFailed("temporary error")
	if err != nil {
		t.Errorf("Failed to mark task as failed: %v", err)
	}

	if !task.ShouldRetry() {
		t.Error("Task should be eligible for retry")
	}

	// Test retry preparation
	err = task.PrepareForRetry(5)
	if err != nil {
		t.Errorf("Failed to prepare task for retry: %v", err)
	}

	if task.GetState() != StatePending {
		t.Errorf("Task state should be reset to 'pending', got '%s'", task.GetState())
	}

	if task.NextRetryAt == nil {
		t.Error("NextRetryAt should be set")
	}

	// Simulate max retries
	task.mu.Lock()
	task.RetryCount = 3
	task.State = StateFailed
	task.mu.Unlock()

	if task.ShouldRetry() {
		t.Error("Task should not be eligible for retry after max retries")
	}

	err = task.PrepareForRetry(5)
	if err == nil {
		t.Error("Should not be able to prepare for retry after max retries")
	}
}

func TestTaskOptions(t *testing.T) {
	payload := json.RawMessage(`{"test":"data"}`)
	idempotencyKey := "custom-idempotency-key"
	correlationID := "custom-correlation-id"
	parentTaskID := "parent-task-123"
	priority := 5
	maxRetries := 10
	timeout := 600

	task := NewTask(
		"test_task",
		payload,
		WithIdempotencyKey(idempotencyKey),
		WithCorrelationID(correlationID),
		WithParentTask(parentTaskID),
		WithPriority(priority),
		WithMaxRetries(maxRetries),
		WithTimeout(timeout),
		WithMetadata("key1", "value1"),
		WithMetadata("key2", "value2"),
	)

	if task.GetIdempotencyKey() != idempotencyKey {
		t.Errorf("Idempotency key should be '%s', got '%s'", idempotencyKey, task.GetIdempotencyKey())
	}

	if task.CorrelationID != correlationID {
		t.Errorf("Correlation ID should be '%s', got '%s'", correlationID, task.CorrelationID)
	}

	if task.ParentTaskID != parentTaskID {
		t.Errorf("Parent task ID should be '%s', got '%s'", parentTaskID, task.ParentTaskID)
	}

	if task.Priority != priority {
		t.Errorf("Priority should be %d, got %d", priority, task.Priority)
	}

	if task.MaxRetries != maxRetries {
		t.Errorf("Max retries should be %d, got %d", maxRetries, task.MaxRetries)
	}

	if task.Timeout != timeout {
		t.Errorf("Timeout should be %d, got %d", timeout, task.Timeout)
	}

	value, exists := task.GetMetadata("key1")
	if !exists || value != "value1" {
		t.Error("Metadata was not properly set or retrieved")
	}
}

func TestConcurrentAccess(t *testing.T) {
	payload := json.RawMessage(`{"test":"data"}`)
	task := NewTask("test_task", payload, WithMaxRetries(10))

	// Start the task
	err := task.MarkStarted()
	if err != nil {
		t.Errorf("Failed to mark task as started: %v", err)
	}

	// Test concurrent metadata access
	var wg sync.WaitGroup
	concurrency := 10

	// Concurrent writers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			key := "key" + string(rune('A'+index))
			value := "value" + string(rune('A'+index))
			task.SetMetadata(key, value)
		}(i)
	}

	// Concurrent readers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = task.GetState()
			_ = task.GetID()
			_ = task.GetType()
			_ = task.GetPayload()
		}()
	}

	wg.Wait()

	// Verify metadata was set correctly
	for i := 0; i < concurrency; i++ {
		key := "key" + string(rune('A'+i))
		expectedValue := "value" + string(rune('A'+i))
		value, exists := task.GetMetadata(key)
		if !exists || value != expectedValue {
			t.Errorf("Metadata for %s was not set correctly", key)
		}
	}
}

func TestTaskClone(t *testing.T) {
	payload := json.RawMessage(`{"test":"data"}`)
	original := NewTask("test_task", payload,
		WithMetadata("key1", "value1"),
		WithMaxRetries(5),
	)

	// Mark as started
	err := original.MarkStarted()
	if err != nil {
		t.Errorf("Failed to mark task as started: %v", err)
	}

	// Clone the task
	clone := original.Clone()

	// Verify clone has the same values
	if clone.GetID() != original.GetID() {
		t.Error("Clone ID doesn't match original")
	}

	if clone.GetState() != original.GetState() {
		t.Error("Clone state doesn't match original")
	}

	// Modify the clone
	err = clone.MarkCompleted()
	if err != nil {
		t.Errorf("Failed to mark clone as completed: %v", err)
	}

	// Verify original is unchanged
	if original.GetState() != StateRunning {
		t.Error("Original task state was modified when clone was changed")
	}

	// Verify clone was changed
	if clone.GetState() != StateCompleted {
		t.Error("Clone state was not updated correctly")
	}
}

func TestTaskExpiration(t *testing.T) {
	payload := json.RawMessage(`{"test":"data"}`)
	task := NewTask("test_task", payload, WithTimeout(1)) // 1 second timeout

	err := task.MarkStarted()
	if err != nil {
		t.Errorf("Failed to mark task as started: %v", err)
	}

	// Task should not be expired immediately
	if task.IsExpired() {
		t.Error("Task should not be expired immediately after starting")
	}

	// Wait for the task to expire
	time.Sleep(1100 * time.Millisecond)

	// Now it should be expired
	if !task.IsExpired() {
		t.Error("Task should be expired after timeout period")
	}

	// Mark as expired
	err = task.MarkExpired()
	if err != nil {
		t.Errorf("Failed to mark task as expired: %v", err)
	}

	if task.GetState() != StateTimeout {
		t.Errorf("Task state should be 'timeout', got '%s'", task.GetState())
	}
}

func TestScheduledTask(t *testing.T) {
	payload := json.RawMessage(`{"test":"data"}`)
	futureTime := time.Now().Add(1 * time.Hour)

	task := NewTask(
		"scheduled_task",
		payload,
		WithScheduledExecution(futureTime),
	)

	if task.GetState() != StateScheduled {
		t.Errorf("Task state should be 'scheduled', got '%s'", task.GetState())
	}

	if task.NextRetryAt == nil || !task.NextRetryAt.Equal(futureTime) {
		t.Error("Scheduled execution time was not properly set")
	}
}
