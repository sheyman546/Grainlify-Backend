package github

import (
	"strings"
	"sync"
	"time"
)

// cacheEntry holds a cached Repo value together with the wall-clock time at
// which it was stored.  expiry is pre-computed at insertion to avoid repeated
// arithmetic on every lookup.
type cacheEntry struct {
	repo   Repo
	expiry time.Time
}

// RepoCache is a concurrency-safe, TTL-based in-memory cache for GitHub
// repo-metadata responses.
//
// # Design choices
//
//   - sync.RWMutex: reads are the common path; writes (inserts + eviction scans)
//     are infrequent, so an RW lock is more efficient than a plain Mutex.
//
//   - Lazy eviction: expired entries are removed during Get calls and during
//     periodic background sweeps started by NewRepoCache.  This avoids
//     allocating a goroutine per-entry and keeps the implementation simple.
//
//   - Key normalisation: keys are stored as lower-case "owner/repo" strings so
//     that lookup and insertion are case-insensitive, matching GitHub's own
//     canonical form.
//
// # Security note
//
// Cached metadata includes repo visibility and permission flags.  A short TTL
// (≤5 min, default 60 s) limits the window in which a since-revoked integration
// could continue operating on stale data.  Callers with freshness requirements
// (e.g. right after an App install/uninstall) must pass bypass=true to
// GetRepoWithCache so the cache is skipped and the entry is unconditionally
// refreshed.
type RepoCache struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry
	ttl     time.Duration
	now     func() time.Time // injectable for testing
}

// NewRepoCache returns a new RepoCache with the given TTL.  A background
// goroutine is started to periodically sweep expired entries; it stops when
// stopCh is closed.  Pass a nil channel (or a channel you never close) if you
// do not need controlled shutdown — the goroutine is cheap and only allocates
// during sweeps.
//
// If ttl ≤ 0 the cache is effectively disabled: every Get misses and every set
// is a no-op.
func NewRepoCache(ttl time.Duration, stopCh <-chan struct{}) *RepoCache {
	c := &RepoCache{
		entries: make(map[string]cacheEntry),
		ttl:     ttl,
		now:     time.Now,
	}
	go c.evictLoop(stopCh)
	return c
}

// cacheKey normalises a full repo name to a canonical lower-case key.
func cacheKey(fullName string) string {
	return strings.ToLower(strings.TrimSpace(fullName))
}

// Get returns the cached Repo for fullName if a fresh (non-expired) entry
// exists.  The second return value is true on a cache hit and false on a miss.
func (c *RepoCache) Get(fullName string) (Repo, bool) {
	if c.ttl <= 0 {
		return Repo{}, false
	}
	key := cacheKey(fullName)

	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return Repo{}, false
	}
	if c.now().After(entry.expiry) {
		// Lazy eviction: remove the stale entry and report a miss.
		c.mu.Lock()
		// Re-check inside the write lock to avoid a race with a concurrent set.
		if e, still := c.entries[key]; still && c.now().After(e.expiry) {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return Repo{}, false
	}
	return entry.repo, true
}

// set stores repo under fullName with a TTL-derived expiry.  It is a no-op
// when the cache TTL is ≤ 0.
func (c *RepoCache) set(fullName string, repo Repo) {
	if c.ttl <= 0 {
		return
	}
	key := cacheKey(fullName)
	c.mu.Lock()
	c.entries[key] = cacheEntry{
		repo:   repo,
		expiry: c.now().Add(c.ttl),
	}
	c.mu.Unlock()
}

// Invalidate removes the entry for fullName from the cache.  It is safe to
// call even when no entry exists.
func (c *RepoCache) Invalidate(fullName string) {
	key := cacheKey(fullName)
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// Len returns the number of entries currently in the cache (including entries
// that have expired but have not yet been evicted).
func (c *RepoCache) Len() int {
	c.mu.RLock()
	n := len(c.entries)
	c.mu.RUnlock()
	return n
}

// evictLoop runs in the background and removes expired entries on each tick.
// The sweep interval is half the TTL to keep memory bounded without sweeping
// too aggressively.  A minimum interval of 5 s is enforced so that very short
// TTLs (e.g. in tests) do not create a busy loop.
func (c *RepoCache) evictLoop(stopCh <-chan struct{}) {
	if c.ttl <= 0 {
		return
	}
	interval := c.ttl / 2
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.evictExpired()
		case <-stopCh:
			return
		}
	}
}

// evictExpired performs a single full sweep, deleting all expired entries.
func (c *RepoCache) evictExpired() {
	now := c.now()
	c.mu.Lock()
	for k, e := range c.entries {
		if now.After(e.expiry) {
			delete(c.entries, k)
		}
	}
	c.mu.Unlock()
}
