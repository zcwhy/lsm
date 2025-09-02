package lsm

import (
	"lsm/entry"
	"os"
	"testing"
)

func TestWrite(t *testing.T) {
	entries := []*entry.Entry{
		{Key: "a", Val: []byte("aaaa"), Deleted: false},
		{Key: "b", Val: []byte("bbbb"), Deleted: false},
	}

	t.Log(WriteToSStable(entries, "./sstable"))
}

func TestLoadMeta(t *testing.T) {
	s := &SSTable{MetaBlock: &MetaInfo{}}
	s.f, _ = os.Open("./sstable")
	s.LoadMetaBlock()
	t.Log(s.MetaBlock)
}

func TestRead(t *testing.T) {
	s := LoadSStableFromFile("./sstable")

	t.Log(s.IndexBlock)
}

func TestFile(t *testing.T) {
	f, _ := os.Open("./sstable")
	info, _ := f.Stat()
	t.Log(info.Size())
}
