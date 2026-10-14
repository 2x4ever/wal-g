package yckms

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
)

// Literal on-disk format: "yckms", version 1, little-endian key length 4,
// then four ciphertext bytes. Expectations do not use the serializer.
var rangeReaderKeyHeader = []byte{'y', 'c', 'k', 'm', 's', 1, 4, 0, 0, 0, 0x11, 0x22, 0x33, 0x44}

// RangeReader fills each header field despite fragmented body reads, and
// leaves the encrypted payload at its exact original offset.
func TestDeserializeEncryptedKeyOverRangeReader(t *testing.T) {
	payload := bytes.Repeat([]byte("encrypted-payload"), 1024)
	encoded := append(append([]byte(nil), rangeReaderKeyHeader...), payload...)
	for _, tt := range []struct {
		name      string
		firstRead int
	}{
		{name: "one_byte_body_reads"},
		{name: "tail_leaves_one_header_byte", firstRead: 4097},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var source io.Reader = iotest.OneByteReader(bytes.NewReader(encoded))
			if tt.firstRead != 0 {
				source = io.MultiReader(bytes.NewReader(encoded[:tt.firstRead]), bytes.NewReader(encoded[tt.firstRead:]))
			}
			folder := walgs3.NewFolder(nil, nil, "", &walgs3.Config{Bucket: "bucket"})
			reader := walgs3.NewRangeReader(t.Context(), io.NopCloser(source), "object", 0, folder)
			t.Cleanup(func() { require.NoError(t, reader.Close()) })

			key, err := deserializeEncryptedKey(reader)
			require.NoError(t, err, "fragmented body reads must not reject a valid YC header")
			require.Equal(t, []byte{0x11, 0x22, 0x33, 0x44}, key)
			remaining, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, payload, remaining, "header reads must preserve encrypted payload bytes")
		})
	}
}
