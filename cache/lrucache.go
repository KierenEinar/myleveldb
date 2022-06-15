package cache

import "myleveldb/collections"

// NamespaceCache 带有ns的cache
type NamespaceCache struct {
	Cache *collections.LRUCache
	Ns    uint32
}

func (ns *NamespaceCache) Get(key []byte, sf collections.SetFunc) (*collections.LRUHandle, error) {
	return ns.Cache.Get(ns.Ns, key, sf)
}
