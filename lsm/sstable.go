package lsm

import (
	"fmt"
	"io"
	"os"
)

type SSTable struct {
	f *os.File

	FilePath   string
	IndexBlock map[string]*Position // key to DataBlock index
	MetaBlock  *MetaInfo
}

type Position struct {
	Len int64
	Off int64
}

// 16bytes
type MetaInfo struct {
	IndexBlockOffset int64
	IndexBlockLen    int64
}

func LoadSStableFromFile(filePath string) *SSTable {
	s := &SSTable{
		FilePath: filePath,
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil
	}

	s.f = f

	s.LoadMetaBlock()
	s.LoadMetaBlock()

	return s
}

func (s *SSTable) LoadMetaBlock() {
	info, err := s.f.Stat()
	if err != nil {
		panic(err)
	}
	size := info.Size()

	// 确保文件至少16字节
	if size < 16 {
		panic("file too small")
	}

	// 从文件末尾向前移动16字节
	_, err = s.f.Seek(-16, io.SeekEnd)
	if err != nil {
		panic(err)
	}

	// 读取16字节
	buf := make([]byte, 16)
	_, err = s.f.Read(buf)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Last 16 bytes: % x\n", buf)
}

func (s *SSTable) LoadIndex() {
}
