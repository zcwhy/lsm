package lsm

type MemTable interface {
	Get(key string) ([]byte, bool)
	Put(key string, value []byte)
	All() []*Entry
}
