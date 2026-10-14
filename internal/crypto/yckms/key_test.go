package yckms

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializeDeserializeKeyHeader(t *testing.T) {
	buffer := new(bytes.Buffer)

	encryptedKey := make([]byte, 64)
	for i := range encryptedKey {
		encryptedKey[i] = 0xaa
	}

	serializedKey := serializeEncryptedKey(encryptedKey)
	buffer.Write(serializedKey)

	deserializedKey, err := deserializeEncryptedKey(buffer)
	assert.NoErrorf(t, err, "YcKms key deserialization error: %v", err)

	assert.Equal(t, len(encryptedKey), len(deserializedKey), "YcKms deserialized key len is not equal to the original one")

	for i := range encryptedKey {
		assert.Equal(t, encryptedKey[i], deserializedKey[i], "YcKms deserialized key is not equal to the original one in position: %d", i)
	}
}

// Literal on-disk format: "yckms", version 1, little-endian key length 4,
// then four ciphertext bytes. Expectations do not use the serializer.
var shortReadKeyHeader = []byte{'y', 'c', 'k', 'm', 's', 1, 4, 0, 0, 0, 0x11, 0x22, 0x33, 0x44}

type keyHeaderReadLimits struct {
	io.Reader
	limits []int
}

func (r *keyHeaderReadLimits) Read(p []byte) (int, error) {
	if len(r.limits) > 0 {
		limit := r.limits[0]
		r.limits = r.limits[1:]
		if len(p) > limit {
			p = p[:limit]
		}
	}
	return r.Reader.Read(p)
}

func TestDeserializeEncryptedKeyShortReads(t *testing.T) {
	for _, test := range []struct {
		name   string
		limits []int
	}{
		{name: "magic", limits: []int{1, 5, 4, 4}},
		{name: "version", limits: []int{5, 1, 4, 4}},
		{name: "length", limits: []int{6, 1, 3, 4}},
		{name: "encrypted key", limits: []int{6, 4, 1, 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded := append(append([]byte(nil), shortReadKeyHeader...), []byte("payload")...)
			reader := &keyHeaderReadLimits{Reader: bytes.NewReader(encoded), limits: test.limits}
			key, err := deserializeEncryptedKey(reader)
			require.NoError(t, err)
			require.Equal(t, []byte{0x11, 0x22, 0x33, 0x44}, key)
			remaining, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, "payload", string(remaining), "header parser must not consume encrypted payload bytes")
		})
	}
}

func TestDeserializeEncryptedKeyTruncatedFields(t *testing.T) {
	for _, test := range []struct {
		name string
		size int
		want error
	}{
		{name: "empty magic", size: 0, want: io.EOF},
		{name: "partial magic", size: 1, want: io.ErrUnexpectedEOF},
		{name: "missing version", size: 5, want: io.ErrUnexpectedEOF},
		{name: "empty length", size: 6, want: io.EOF},
		{name: "partial length", size: 7, want: io.ErrUnexpectedEOF},
		{name: "last length byte missing", size: 9, want: io.ErrUnexpectedEOF},
		{name: "empty encrypted key", size: 10, want: io.EOF},
		{name: "partial encrypted key", size: 11, want: io.ErrUnexpectedEOF},
		{name: "last key byte missing", size: 13, want: io.ErrUnexpectedEOF},
	} {
		t.Run(test.name, func(t *testing.T) {
			key, err := deserializeEncryptedKey(bytes.NewReader(shortReadKeyHeader[:test.size]))
			require.ErrorIs(t, err, test.want)
			require.Nil(t, key, "incomplete fields must not produce a zero-padded key")
		})
	}
}

func TestDeserializeEncryptedKeyRejectsInvalidFields(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func([]byte)
		want   string
	}{
		{name: "magic", change: func(header []byte) { header[0] = 'x' }, want: "invalid encrypted header format"},
		{name: "version", change: func(header []byte) { header[5] = 2 }, want: "scheme version is not supported"},
		{name: "oversized key", change: func(header []byte) { header[6], header[7] = 1, 0x10 }, want: "invalid size of the encrypted key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := append([]byte(nil), shortReadKeyHeader...)
			test.change(header)
			reader := &keyHeaderReadLimits{Reader: bytes.NewReader(header), limits: []int{1, 5, 1, 3, 1, 3}}
			key, err := deserializeEncryptedKey(reader)
			require.ErrorContains(t, err, test.want)
			require.Nil(t, key)
		})
	}
}
