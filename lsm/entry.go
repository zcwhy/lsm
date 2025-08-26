package lsm

import (
	"encoding/json"
	"fmt"
)

type Entry struct {
	Key     string
	Val     []byte
	Deleted bool
}

func NewEntry(key string, val []byte, deleted bool) *Entry {
	return &Entry{
		Key:     key,
		Val:     val,
		Deleted: deleted,
	}
}

func (e *Entry) Encode() ([]byte, error) {
	return json.Marshal(e)
}

func Decode(data []byte) (*Entry, error) {
	var e Entry
	err := json.Unmarshal(data, &e)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// String 方法，自定义序列化
func (e Entry) String() string {
	// 定义一个匿名结构体，用于序列化
	aux := struct {
		Key     string `json:"Key"`
		Val     string `json:"Val"` // 转为 string
		Deleted bool   `json:"Deleted"`
	}{
		Key:     e.Key,
		Val:     string(e.Val), // 直接转字符串
		Deleted: e.Deleted,
	}

	data, err := json.Marshal(aux)
	if err != nil {
		return fmt.Sprintf("Entry{Error: %v}", err)
	}
	return string(data)
}
