package lsm

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"lsm/entry"
	"os"
)

type SSTable struct {
	f *os.File

	FilePath   string
	IndexBlock map[string]*Position // key to DataBlock index
	MetaBlock  *MetaInfo
}

type Position struct {
	Len uint64
	Off uint64
}

// 16bytes
type MetaInfo struct {
	IndexBlockOffset uint64
	IndexBlockLen    uint64
}

func LoadSStableFromFile(filePath string) *SSTable {
	s := &SSTable{
		FilePath:   filePath,
		IndexBlock: make(map[string]*Position),
		MetaBlock:  &MetaInfo{},
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil
	}

	s.f = f

	s.LoadMetaBlock()
	s.LoadIndexBlock()

	return s
}

func WriteToSStable(datas []*entry.Entry, filePath string) (*SSTable, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	w := bufio.NewWriterSize(f, 4096)

	s := &SSTable{
		IndexBlock: make(map[string]*Position),
		MetaBlock:  new(MetaInfo),
	}

	var off uint64
	// write data block, build index info
	for _, e := range datas {
		bytes, err := e.Encode()
		if err != nil {
			return nil, err
		}

		n, err := w.Write(bytes)
		if err != nil {
			return nil, err
		}

		s.IndexBlock[e.Key] = &Position{Off: off, Len: uint64(n)}
		off += uint64(n)
	}

	// write index block
	indexBytes, err := json.Marshal(s.IndexBlock)
	if err != nil {
		return nil, err
	}
	n, err := w.Write(indexBytes)
	if err != nil {
		return nil, err
	}

	s.MetaBlock.IndexBlockOffset = off
	s.MetaBlock.IndexBlockLen = uint64(n)

	// write meta block
	metaBytes := s.MetaBlock.Encode()
	_, err = w.Write(metaBytes)
	if err != nil {
		return nil, err
	}
	return s, w.Flush()
}

func (s *SSTable) LoadMetaBlock() error {
	info, err := s.f.Stat()
	if err != nil {
		return err
	}

	// 确保文件至少16字节
	if info.Size() < 16 {
		return errors.New("Invalid meta size")
	}

	// 从文件末尾向前移动16字节
	_, err = s.f.Seek(-16, io.SeekEnd)
	if err != nil {
		return err
	}

	// 读取16字节
	buf := make([]byte, 16)
	_, err = s.f.Read(buf)
	if err != nil {
		return err
	}

	return s.MetaBlock.Decode(buf)
}

func (s *SSTable) LoadIndexBlock() error {
	idxOff := s.MetaBlock.IndexBlockOffset
	idxBytes := make([]byte, s.MetaBlock.IndexBlockLen)

	_, err := s.f.ReadAt(idxBytes, int64(idxOff))
	if err != nil {
		return err
	}

	if err := json.Unmarshal(idxBytes, &s.IndexBlock); err != nil {
		return err
	}
	return nil
}

func (m *MetaInfo) Encode() []byte {
	metaBytes := make([]byte, 16)

	binary.BigEndian.PutUint64(metaBytes[0:8], uint64(m.IndexBlockLen))
	binary.BigEndian.PutUint64(metaBytes[8:16], uint64(m.IndexBlockOffset))

	return metaBytes
}

func (m *MetaInfo) Decode(metaBytes []byte) error {
	if len(metaBytes) != 16 {
		return errors.New("Invalid metainfo length")
	}

	m.IndexBlockLen = binary.BigEndian.Uint64(metaBytes[0:8])
	m.IndexBlockOffset = binary.BigEndian.Uint64(metaBytes[8:16])

	return nil
}
