package lsm

type Engine struct {
	config           *Config
	memTable         MemTable
	iMutableTable    chan MemTable
	nodes            [][]*Node
	levelCompactChan chan int
}

func NewEngine() *Engine {
	e := &Engine{}

	go e.flushIMutableTable()
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

		if level == 0 {
			e.compactLevel0()
			continue
		}
		e.compactLevel(level)
	}
}

func (e *Engine) compactLevel0() {

}

func (e *Engine) compactLevel(level int) {

}
