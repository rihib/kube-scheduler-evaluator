package clock

import (
	"sync"
	"time"
)

var (
	mu          sync.RWMutex
	virtualTime time.Time
	updatedAt   time.Time // real
)

func VirtualTime() time.Time {
	mu.RLock()
	defer mu.RUnlock()
	return virtualTime
}

func SetVirtualTime(t time.Time) {
	mu.Lock()
	defer mu.Unlock()
	virtualTime = t
	updatedAt = time.Now()
}

func UpdatedAt() time.Time {
	mu.RLock()
	defer mu.RUnlock()
	return updatedAt
}
