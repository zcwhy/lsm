package lsm

import (
	"fmt"
	"lsm/memtable"
	"strings"
	"sync/atomic"
)

type Engine struct {
	config           *Config
	memTable         memtable.MemTable
	iMutableTable    chan memtable.MemTable
	nodes            [][]*Node
	levelCompactChan chan int
	levelSeq         []atomic.Int32 // 各层 sstable 文件 seq. sstable 文件命名为 level_seq.sst
}

func NewEngine() *Engine {
	e := &Engine{}

	go e.flushIMutableTable()

	return e
}

func (e *Engine) Get() {

}

func (e *Engine) Put() {

}

func (e *Engine) flushIMutableTable() {
	for table := range e.iMutableTable {
		// get all values
		kvs := table.All()

		// write to SStable

		// build node
		node := NewNode(0, kvs[0].Key, kvs[len(kvs)-1].Key)

		e.insertNode(node, 0)

		// try compaction
		go e.tryCompact(0)
	}
}

func (e *Engine) insertNode(node *Node, level int) {
	// 晚加入的node append在后边
	if level == 0 {
		e.nodes[0] = append(e.nodes[0], node)
	}
}

// decide if a leve sstables need compaction
func (e *Engine) tryCompact(level int) {
	levelNodes := e.nodes[level]

	if len(levelNodes) <= e.config.LevelSize[level] {
		return
	}

	e.levelCompactChan <- level
}

func (e *Engine) Compact() {
	for {
		level := <-e.levelCompactChan

		e.compactLevelx(level)
	}
}

func (e *Engine) compactLevelx(level int) {
	compactNode := e.nodes[level][0]
	overlapNodes := e.findOverLapNodes(compactNode)
	memtable := e.compactNodes(overlapNodes)

	// write to + 1 level
	kvs := memtable.All()
	_, err := WriteToSStable(kvs, ToSSTFileName(level+1, e.getLevelSeq(level+1)))
	if err != nil {

	}
	e.insertNode(&Node{StartKey: kvs[0].Key, EndKey: kvs[len(kvs)-1].Key}, level+1)

	// clean old nodes
	e.removeNodes(overlapNodes)

	// try to triiger level + 1 compact
	e.tryCompact(level + 1)
}

func (e *Engine) compactNodes(overlapNodes []*Node) memtable.MemTable {
	var compatTable memtable.MemTable = memtable.NewTree()
	for _, node := range overlapNodes {
		nodeEntries := node.GetAllEntrie()
		for _, entry := range nodeEntries {
			compatTable.Put(entry.Key, entry.Val)
		}
	}

	return compatTable
}

// find level and level + 1 overlap nodes
// The order of slice is node priority order
func (e *Engine) findOverLapNodes(compactNode *Node) []*Node {
	var overlapNodes []*Node
	if compactNode.Level == 0 {
		for _, node := range e.nodes[compactNode.Level] {
			if node == compactNode {
				continue
			}
			if isOverLap(compactNode, node) {
				overlapNodes = append(overlapNodes, node)
			}
		}
	}

	for _, node := range e.nodes[compactNode.Level+1] {
		if isOverLap(compactNode, node) {
			overlapNodes = append(overlapNodes, node)
		}
	}

	return overlapNodes
}

func (e *Engine) getLevelSeq(level int) int {
	seq := e.levelSeq[level+1].Load() + 1
	e.levelSeq[level+1].Store(seq)

	return int(seq)
}

func isOverLap(node1, node2 *Node) bool {
	smallNode, largerNode := node1, node2
	if strings.Compare(node1.StartKey, node2.StartKey) == 1 {
		smallNode = node2
		largerNode = node1
	}
	return !(largerNode.StartKey > smallNode.EndKey)
}

func ToSSTFileName(level, seq int) string {
	return fmt.Sprintf("%d_%d.sst", level, seq)
}
