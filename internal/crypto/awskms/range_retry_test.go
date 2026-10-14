package awskms

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"testing/iotest"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
)

type encryptedRangeAPI struct {
	walgs3.API
	get func(*awss3.GetObjectInput) (*awss3.GetObjectOutput, error)
}

func (a *encryptedRangeAPI) GetObject(_ context.Context, input *awss3.GetObjectInput,
	_ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	return a.get(input)
}

type encryptedRangeReadError struct{}

func (encryptedRangeReadError) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func TestDecryptRangeRetryDiscardsErrorText(t *testing.T) {
	plaintext := bytes.Repeat([]byte("encrypted backup data"), 5000)
	var encrypted bytes.Buffer
	writer, err := MockCrypterFromKeyID("test-key").Encrypt(t.Context(), &encrypted)
	require.NoError(t, err)
	_, err = writer.Write(plaintext)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	ciphertext := encrypted.Bytes()

	const prefixSize, junkSize, rollback = 12288, 300, 4096
	partial := append(append([]byte(nil), ciphertext[:prefixSize]...), bytes.Repeat([]byte{'!'}, junkSize)...)
	// The first body read leaves only one byte above the retained tail.
	// RangeReader must fill the key header before returning it to the crypter.
	// Later successful body reads include all injected error text.
	body := io.NopCloser(io.MultiReader(
		bytes.NewReader(partial[:4097]),
		bytes.NewReader(partial[4097:]),
		encryptedRangeReadError{},
	))
	const offset = prefixSize + junkSize - rollback
	gets := 0
	api := &encryptedRangeAPI{get: func(input *awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
		gets++
		require.Equal(t, 1, gets, "only one ranged retry is needed")
		require.Equal(t, "bytes=8492-", aws.ToString(input.Range))
		return &awss3.GetObjectOutput{
			Body:          io.NopCloser(bytes.NewReader(ciphertext[offset:])),
			ContentLength: aws.Int64(int64(len(ciphertext) - offset)),
			ContentRange:  aws.String(fmt.Sprintf("bytes %d-%d/%d", offset, len(ciphertext)-1, len(ciphertext))),
		}, nil
	}}
	folder := walgs3.NewFolder(api, nil, "", &walgs3.Config{Bucket: "test-bucket"})
	rangeReader := walgs3.NewRangeReader(t.Context(), body, "encrypted-object", 1, folder)
	t.Cleanup(func() { require.NoError(t, rangeReader.Close()) })

	reader, err := MockCrypterFromKeyID("test-key").Decrypt(t.Context(), rangeReader)
	require.NoError(t, err)
	actual, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, plaintext, actual)
	require.Equal(t, 1, gets)
}

// The KMS parser reads its header once. RangeReader must fill that read even
// when the healthy HTTP body is fragmented around the retained 4096-byte tail.
func TestDecryptFragmentedBodyOverRangeReader(t *testing.T) {
	plaintext := bytes.Repeat([]byte("encrypted backup data"), 512)
	var encrypted bytes.Buffer
	writer, err := MockCrypterFromKeyID("test-key").Encrypt(t.Context(), &encrypted)
	require.NoError(t, err)
	_, err = writer.Write(plaintext)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	ciphertext := encrypted.Bytes()

	for _, tt := range []struct {
		name      string
		firstRead int
	}{
		{name: "one_byte_body_reads"},
		{name: "tail_leaves_one_header_byte", firstRead: 4097},
		{name: "tail_leaves_partial_header", firstRead: 4279},
		{name: "tail_leaves_complete_header", firstRead: 4280},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var source io.Reader = iotest.OneByteReader(bytes.NewReader(ciphertext))
			if tt.firstRead != 0 {
				source = io.MultiReader(
					bytes.NewReader(ciphertext[:tt.firstRead]),
					bytes.NewReader(ciphertext[tt.firstRead:]),
				)
			}
			folder := walgs3.NewFolder(nil, nil, "", &walgs3.Config{})
			rangeReader := walgs3.NewRangeReader(t.Context(), io.NopCloser(source), "encrypted-object", 0, folder)
			t.Cleanup(func() { require.NoError(t, rangeReader.Close()) })

			crypter := &Crypter{SymmetricKey: NewMockSymmetricKey("test-key", 32, 184)}
			reader, err := crypter.Decrypt(t.Context(), rangeReader)
			require.NoError(t, err)
			require.Equal(t, ciphertext[:184], crypter.SymmetricKey.GetEncryptedKey(),
				"the single header read must receive the exact encrypted key")
			actual, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, plaintext, actual)
		})
	}
}
