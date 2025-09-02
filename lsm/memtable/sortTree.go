package memtable

import (
	"lsm/entry"
	"sync"
)

type treeNode struct {
	KV    entry.Entry
	Left  *treeNode
	Right *treeNode
}

type Tree struct {
	root   *treeNode
	count  int
	RWLock *sync.RWMutex
}

func NewTree() *Tree {
	return &Tree{}
}

func (tree *Tree) Init() {
	tree.RWLock = &sync.RWMutex{}
}

func (tree *Tree) Count() int {
	return tree.count
}

func (tree *Tree) GetCount() int {
	return tree.count
}

// Set 设置 Key 的值并返回旧值
func (tree *Tree) Get(key string) ([]byte, bool) {}

func (tree *Tree) Put(key string, value []byte)

func (tree *Tree) Delete(key string)

func (tree *Tree) All() []*entry.Entry
