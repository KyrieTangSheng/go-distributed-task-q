package memory

import (
	"container/heap"
	"sync"
	"time"
)

// taskQueueItem represents a task in the priority queue
type taskQueueItem struct {
	id       string
	priority int
	readyAt  time.Time
	index    int // Used by heap.Interface
}

// taskQueue is a thread-safe priority queue for task IDs
type taskQueue struct {
	mu    sync.Mutex
	items []*taskQueueItem
}

// newTaskQueue creates a new task queue
func newTaskQueue() *taskQueue {
	q := &taskQueue{
		items: make([]*taskQueueItem, 0),
	}
	heap.Init(q)
	return q
}

// Push adds a task to the queue
func (q *taskQueue) PushTask(id string, priority int, readyAt time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()

	item := &taskQueueItem{
		id:       id,
		priority: priority,
		readyAt:  readyAt,
	}
	heap.Push(q, item)
}

// Pop removes and returns the highest priority task that's ready to run
// Returns empty string if no tasks are ready
func (q *taskQueue) PopTask() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.Len() == 0 {
		return "", false
	}

	// Check if the highest priority task is ready
	item := q.items[0]
	if time.Now().Before(item.readyAt) {
		return "", false // No tasks ready yet
	}

	// Remove the item from the heap
	item = heap.Pop(q).(*taskQueueItem)
	return item.id, true
}

// Remove removes a task from the queue
func (q *taskQueue) RemoveTask(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i, item := range q.items {
		if item.id == id {
			heap.Remove(q, i)
			return true
		}
	}
	return false
}

// Len returns the number of items in the queue
func (q *taskQueue) Len() int {
	return len(q.items)
}

// Less compares items for the heap interface
// Higher priority tasks come first
// For equal priority, earlier readyAt time comes first
func (q *taskQueue) Less(i, j int) bool {
	if q.items[i].priority != q.items[j].priority {
		return q.items[i].priority > q.items[j].priority
	}
	return q.items[i].readyAt.Before(q.items[j].readyAt)
}

// Swap swaps items for the heap interface
func (q *taskQueue) Swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
	q.items[i].index = i
	q.items[j].index = j
}

// Push implements heap.Interface
func (q *taskQueue) Push(x interface{}) {
	item := x.(*taskQueueItem)
	item.index = len(q.items)
	q.items = append(q.items, item)
}

// Pop implements heap.Interface
func (q *taskQueue) Pop() interface{} {
	old := q.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // Avoid memory leak
	item.index = -1 // For safety
	q.items = old[0 : n-1]
	return item
}
