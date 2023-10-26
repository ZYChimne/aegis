package hotkey

import (
	"fmt"
	"math"
	"regexp"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/zychimne/aegis/topk"
)

type CacheRuleConfig struct {
	Mode  string        `toml:"match_mode"`
	Value string        `toml:"match_value"`
	TTL   time.Duration `toml:"ttl"`
}

type Option struct {
	HotKeyCnt     int
	LocalCacheCap uint64
	AutoCache     bool
	TTL           time.Duration
	MinCount      int
	WhileList     []*CacheRuleConfig
	BlackList     []*CacheRuleConfig
}

var (
	ruleTypeKey     = "key"
	ruleTypePattern = "pattern"
)

type cacheRule struct {
	value  string
	regexp *regexp.Regexp
	ttl    time.Duration
}

type HotKeyWithCache struct {
	topk       topk.Topk
	mutex      sync.Mutex
	option     *Option
	localCache *ttlcache.Cache[string, interface{}]
	whilelist  []*cacheRule
	blacklist  []*cacheRule
}

func NewHotkey(option *Option) (*HotKeyWithCache, error) {
	var err error
	h := &HotKeyWithCache{option: option}
	if option.HotKeyCnt > 0 {
		factor := uint32(math.Log(float64(option.HotKeyCnt)))
		if factor < 1 {
			factor = 1
		}
		h.topk = topk.NewHeavyKeeper(uint32(option.HotKeyCnt), 1024*factor, 4, 0.925, uint32(option.MinCount))
	}
	if len(h.option.WhileList) > 0 {
		h.whilelist, err = h.initCacheRules(h.option.WhileList)
		if err != nil {
			return nil, err
		}
	}
	if len(h.option.BlackList) > 0 {
		h.blacklist, err = h.initCacheRules(h.option.BlackList)
		if err != nil {
			return nil, err
		}
	}
	if h.option.AutoCache || len(h.whilelist) > 0 {
		h.localCache = ttlcache.New[string, interface{}](
			ttlcache.WithCapacity[string, interface{}](h.option.LocalCacheCap),
		)
	}
	return h, nil
}

func (h *HotKeyWithCache) initCacheRules(rules []*CacheRuleConfig) ([]*cacheRule, error) {
	list := make([]*cacheRule, 0, len(rules))
	for _, rule := range rules {
		ttl := rule.TTL
		if ttl == 0 {
			ttl = h.option.TTL
		}
		cacheRule := &cacheRule{ttl: ttl}
		if rule.Mode == ruleTypeKey {
			cacheRule.value = rule.Value
		} else if rule.Mode == ruleTypePattern {
			regexp, err := regexp.Compile(rule.Value)
			if err != nil {
				return nil, fmt.Errorf("localcache: add rule pattern failed, err:%v", err)
			}
			cacheRule.regexp = regexp
		} else {
			return nil, fmt.Errorf("invalid local cache rule mode")
		}
		list = append(list, cacheRule)
	}
	return list, nil
}

func (h *HotKeyWithCache) inBlacklist(key string) bool {
	if len(h.blacklist) == 0 {
		return false
	}
	for _, b := range h.blacklist {
		if b.value == key {
			return true
		}
		if b.regexp != nil && b.regexp.Match([]byte(key)) {
			return true
		}
	}
	return false
}

func (h *HotKeyWithCache) inWhitelist(key string) (time.Duration, bool) {
	if len(h.whilelist) == 0 {
		return 0, false
	}
	for _, b := range h.whilelist {
		if b.value == key {
			return b.ttl, true
		}
		if b.regexp != nil && b.regexp.Match([]byte(key)) {
			return b.ttl, true
		}
	}
	return 0, false
}

// Add add item to topk, and return true if it's hotkey.
func (h *HotKeyWithCache) Add(key string, incr uint32) bool {
	if h.topk == nil {
		return false
	}
	h.mutex.Lock()
	defer h.mutex.Unlock()
	_, hotkey := h.topk.Add(key, incr)
	return hotkey
}

// AddWithValue add item to topk, and return true if it's hotkey.
func (h *HotKeyWithCache) AddWithValue(key string, value interface{}, incr uint32) bool {
	if h.topk == nil && h.localCache == nil {
		return false
	}
	h.mutex.Lock()
	defer h.mutex.Unlock()
	var added bool
	if h.topk != nil {
		var expelled string
		expelled, added = h.topk.Add(key, incr)
		if len(expelled) > 0 && h.localCache != nil {
			h.localCache.Delete(expelled)
		}
		if h.option.AutoCache && added {
			if !h.inBlacklist(key) {
				h.localCache.Set(key, value, h.option.TTL)
			}
			return added
		}
	}
	if ttl, ok := h.inWhitelist(key); ok {
		h.localCache.Set(key, value, ttl)
	}
	return added
}

func (h *HotKeyWithCache) Del(key string) {
	if h.localCache == nil {
		return
	}
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.localCache.Delete(key)
}

func (h *HotKeyWithCache) Get(key string) interface{} {
	if h.localCache == nil {
		return nil
	}
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.localCache.DeleteExpired()
	if item := h.localCache.Get(key); item != nil {
		return item.Value()
	}
	return nil
}

func (h *HotKeyWithCache) Fading() {
	if h.topk == nil {
		return
	}
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.topk.Fading()
}

func (h *HotKeyWithCache) List() []topk.Item {
	if h.topk == nil {
		return nil
	}
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.topk.List()
}
