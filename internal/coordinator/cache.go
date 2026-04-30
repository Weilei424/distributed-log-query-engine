package coordinator

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

type cacheEntry struct {
	key        string
	result     *types.QueryResult
	insertedAt time.Time
	prev, next *cacheEntry
}

// QueryCache is a thread-safe TTL + LRU cache for query results.
type QueryCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	maxSize int
	items   map[string]*cacheEntry
	head    *cacheEntry // most recently used sentinel
	tail    *cacheEntry // least recently used sentinel
}

// NewQueryCache creates a QueryCache with the given TTL and max entry count.
func NewQueryCache(ttl time.Duration, maxSize int) *QueryCache {
	head := &cacheEntry{}
	tail := &cacheEntry{}
	head.next = tail
	tail.prev = head
	return &QueryCache{
		ttl:     ttl,
		maxSize: maxSize,
		items:   make(map[string]*cacheEntry),
		head:    head,
		tail:    tail,
	}
}

// Get returns the cached result for key if it exists and has not expired.
func (c *QueryCache) Get(key string) (*types.QueryResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if time.Since(e.insertedAt) > c.ttl {
		c.remove(e)
		return nil, false
	}
	c.moveToFront(e)
	return e.result, true
}

// Put inserts or updates key with result. Evicts LRU entry if at capacity.
func (c *QueryCache) Put(key string, result *types.QueryResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		e.result = result
		e.insertedAt = time.Now()
		c.moveToFront(e)
		return
	}
	if len(c.items) >= c.maxSize {
		c.evictLRU()
	}
	e := &cacheEntry{key: key, result: result, insertedAt: time.Now()}
	c.items[key] = e
	c.insertFront(e)
}

func (c *QueryCache) remove(e *cacheEntry) {
	e.prev.next = e.next
	e.next.prev = e.prev
	delete(c.items, e.key)
}

func (c *QueryCache) moveToFront(e *cacheEntry) {
	e.prev.next = e.next
	e.next.prev = e.prev
	c.insertFront(e)
}

func (c *QueryCache) insertFront(e *cacheEntry) {
	e.next = c.head.next
	e.prev = c.head
	c.head.next.prev = e
	c.head.next = e
}

func (c *QueryCache) evictLRU() {
	lru := c.tail.prev
	if lru == c.head {
		return
	}
	c.remove(lru)
}

// CacheKey computes a cache key from normalized query parameters.
func CacheKey(queryString, namespace, service string, startTime, endTime int64, limit, offset int32) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s|%d|%d|%d|%d", queryString, namespace, service, startTime, endTime, limit, offset)
	return fmt.Sprintf("%x", h.Sum(nil))
}
