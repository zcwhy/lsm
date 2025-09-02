package lsm

import "testing"

func TestOverLap(t *testing.T) {
	exams := []struct {
		node1   *Node
		node2   *Node
		overlap bool
	}{
		{
			node1: &Node{
				StartKey: "a",
				EndKey:   "c",
			},
			node2: &Node{
				StartKey: "e",
				EndKey:   "h",
			},
			overlap: false,
		},
		{
			node1: &Node{
				StartKey: "a",
				EndKey:   "c",
			},
			node2: &Node{
				StartKey: "b",
				EndKey:   "c",
			},
			overlap: true,
		},
		{
			node1: &Node{
				StartKey: "a",
				EndKey:   "d",
			},
			node2: &Node{
				StartKey: "b",
				EndKey:   "c",
			},
			overlap: true,
		},
		{
			node1: &Node{
				StartKey: "b",
				EndKey:   "c",
			},
			node2: &Node{
				StartKey: "a",
				EndKey:   "d",
			},
			overlap: true,
		},
	}

	for _, exam := range exams {
		result := isOverLap(exam.node1, exam.node2)
		if result != exam.overlap {
			t.Errorf("node1: %v, node2: %v, want overlap result:%v, but get:%v", exam.node1, exam.node2, exam.overlap, result)
		}
	}
}
