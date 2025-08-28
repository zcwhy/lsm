package lsm

type Node struct {
	Level    int
	StartKey string
	EndKey   string
}

func NewNode(level int, startKey, endKey string) *Node {
	return &Node{
		Level:    level,
		StartKey: startKey,
		EndKey:   endKey,
	}
}
