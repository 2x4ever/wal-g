package yckms

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
)

// The first 4097-byte source read leaves only one byte available above the
// RangeReader's retained tail. A header parser must fill each field across
// Read calls, and leave the encrypted payload at its exact original offset.
func TestDeserializeEncryptedKeyOverRangeReader(t *testing.T) {
	payload := bytes.Repeat([]byte("encrypted-payload"), 1024)
	encoded := append(append([]byte(nil), shortReadKeyHeader...), payload...)
	body := io.NopCloser(io.MultiReader(bytes.NewReader(encoded[:4097]), bytes.NewReader(encoded[4097:])))
	folder := walgs3.NewFolder(nil, nil, "", &walgs3.Config{Bucket: "bucket"})
	reader := walgs3.NewRangeReader(t.Context(), body, "object", 0, folder)
	defer reader.Close()

	key, err := deserializeEncryptedKey(reader)
	require.NoError(t, err, "a one-byte first header read must not reject a valid YC header")
	require.Equal(t, []byte{0x11, 0x22, 0x33, 0x44}, key)
	remaining, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, payload, remaining, "header reads must preserve encrypted payload bytes")
}
