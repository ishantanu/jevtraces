package jevtracesprocessor

import (
	"container/list"
	"time"
)

type cacheEntry struct {
	key     string
	value   assessment
	expires time.Time
}

// All cache methods are called with tracesProcessor.mu held.
type assessmentCache struct {
	capacity int
	entries  map[string]*list.Element
	order    *list.List
}

func newCache(capacity int) *assessmentCache {
	return &assessmentCache{capacity: capacity, entries: make(map[string]*list.Element), order: list.New()}
}

func (c *assessmentCache) remove(e *list.Element) {
	delete(c.entries, e.Value.(cacheEntry).key)
	c.order.Remove(e)
}

func (c *assessmentCache) get(key string, now time.Time) (assessment, bool) {
	e, ok := c.entries[key]
	if !ok {
		return assessment{}, false
	}
	entry := e.Value.(cacheEntry)
	if !now.Before(entry.expires) {
		c.remove(e)
		return assessment{}, false
	}
	c.order.MoveToFront(e)
	return entry.value, true
}

func (c *assessmentCache) put(key string, value assessment, expires time.Time) {
	if e, ok := c.entries[key]; ok {
		e.Value = cacheEntry{key, value, expires}
		c.order.MoveToFront(e)
		return
	}
	if c.order.Len() >= c.capacity {
		c.remove(c.order.Back())
	}
	c.entries[key] = c.order.PushFront(cacheEntry{key, value, expires})
}

func (c *assessmentCache) prune(now time.Time) {
	for _, e := range c.entries {
		if !now.Before(e.Value.(cacheEntry).expires) {
			c.remove(e)
		}
	}
}
