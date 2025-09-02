package memtable

import "lsm/entry"

type MemTable interface {
	Get(key string) ([]byte, bool)
	Put(key string, value []byte)
	Delete(key string)
	All() []*entry.Entry
}
