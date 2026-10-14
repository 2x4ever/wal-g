package awskms

import (
	"bytes"
	"context"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/crypto"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
)

type MockSymmetricKey struct {
	SymmetricKey
}

func (symmetricKey *MockSymmetricKey) Encrypt(_ context.Context) error {
	salt := "152 random bytes to imitate aws kms encryption method, random words here: witch collapse practice feed shame open despair creek road again ice least it!"
	symmetricKey.SetEncryptedKey(append(symmetricKey.GetKey(), salt...))
	return nil
}

func (symmetricKey *MockSymmetricKey) Decrypt(_ context.Context) error {
	symmetricKey.SetKey(symmetricKey.GetEncryptedKey()[:symmetricKey.GetKeyLen()])
	return nil
}

func NewMockSymmetricKey(kmsKeyID string, keyLen int, encryptedKeyLen int) *MockSymmetricKey {
	return &MockSymmetricKey{SymmetricKey{SymmetricKeyLen: keyLen, EncryptedSymmetricKeyLen: encryptedKeyLen, KeyID: kmsKeyID}}
}

func MockCrypterFromKeyID(CseKmsID string) crypto.Crypter {
	return &Crypter{SymmetricKey: NewMockSymmetricKey(CseKmsID, 32, 184)}
}

func TestEncryptionCycle(t *testing.T) {
	const someSecret = "so very secret thingy"

	CseKmsID := "AWSKMSKEYID"

	crypter := MockCrypterFromKeyID(CseKmsID)

	buf := new(bytes.Buffer)
	encrypt, err := crypter.Encrypt(t.Context(), buf)
	assert.NoErrorf(t, err, "Encryption error: %v", err)

	encrypt.Write([]byte(someSecret))
	encrypt.Close()

	decrypt, err := crypter.Decrypt(t.Context(), buf)
	assert.NoErrorf(t, err, "Decryption error: %v", err)

	decryptedBytes, err := io.ReadAll(decrypt)
	assert.NoErrorf(t, err, "Decryption read error: %v", err)

	assert.Equal(t, someSecret, string(decryptedBytes), "Decrypted text not equals open text")
}

// The encrypted key header must be read completely even when the source returns
// short successful reads. The real range reader can expose such reads while
// retaining the last 4096 bytes of an otherwise healthy S3 response.
func TestDecryptFragmentedHeader(t *testing.T) {
	plaintext := bytes.Repeat([]byte("encrypted backup data"), 5000)
	var encrypted bytes.Buffer
	writer, err := MockCrypterFromKeyID("test-key").Encrypt(t.Context(), &encrypted)
	require.NoError(t, err)
	_, err = writer.Write(plaintext)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	ciphertext := encrypted.Bytes()

	tests := []struct {
		name      string
		firstRead int
	}{
		{name: "one_byte_reads"},
		{name: "range_tail_leaves_one_header_byte", firstRead: 4097},
		{name: "range_tail_leaves_partial_header", firstRead: 4279},
		{name: "range_tail_leaves_complete_header", firstRead: 4280},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var source io.Reader = iotest.OneByteReader(bytes.NewReader(ciphertext))
			if tt.firstRead != 0 {
				body := io.NopCloser(io.MultiReader(
					bytes.NewReader(ciphertext[:tt.firstRead]),
					bytes.NewReader(ciphertext[tt.firstRead:]),
				))
				folder := walgs3.NewFolder(nil, nil, "", &walgs3.Config{})
				reader := walgs3.NewRangeReader(t.Context(), body, "encrypted-object", 0, folder)
				t.Cleanup(func() { require.NoError(t, reader.Close()) })
				source = reader
			}
			reader, err := MockCrypterFromKeyID("test-key").Decrypt(t.Context(), source)
			require.NoError(t, err)
			actual, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, plaintext, actual)
		})
	}
}
