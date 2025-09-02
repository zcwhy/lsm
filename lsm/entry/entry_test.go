package entry

import "testing"

func TestEntryDecode(t *testing.T) {
	e := NewEntry("key1", []byte("value1"), false)
	data, err := e.Encode()
	if err != nil {
		t.Fatalf("Failed to encode entry: %v", err)
	}
	t.Log(string(data))

	expected := `{"Key":"key1","Val":"dmFsdWUx","Deleted":false}`
	if string(data) != expected {
		t.Errorf("Expected %s, got %s", expected, string(data))
	}
}

func TestDecode(t *testing.T) {
	e := NewEntry("key2", []byte("value2"), true)
	data, err := e.Encode()
	if err != nil {
		t.Fatalf("Failed to encode entry: %v", err)
	}

	dE, err := Decode(data)
	if err != nil {
		t.Fatalf("Failed to decode entry: %v", err)
	}

	t.Log(dE)
}
