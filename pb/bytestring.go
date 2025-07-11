package dht_pb

import (
	"encoding/json"
)

type ByteString string

func (b ByteString) Marshal() ([]byte, error) {
	return []byte(b), nil
}

func (b *ByteString) MarshalTo(data []byte) (int, error) {
	return copy(data, *b), nil
}

func (b *ByteString) Unmarshal(data []byte) error {
	*b = ByteString(data)
	return nil
}

func (b *ByteString) Size() int {
	return len(*b)
}

func (b ByteString) MarshalJSON() ([]byte, error) {
	return json.Marshal([]byte(b))
}

func (b *ByteString) UnmarshalJSON(data []byte) error {
	var buf []byte
	err := json.Unmarshal(data, &buf)
	if err != nil {
		return err
	}
	*b = ByteString(buf)
	return nil
}

func (b ByteString) Equal(other ByteString) bool {
	return b == other
}
