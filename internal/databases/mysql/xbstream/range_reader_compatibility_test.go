package xbstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/compression/zstd"
	s3store "github.com/wal-g/wal-g/pkg/storages/s3"
)

// Fragmentation and a terminal error returned with data are both valid io.Reader
// behaviors. Exercise both through the real parser and extraction sinks.
type s3RangeReadBehavior struct {
	fragmentSize         int
	terminalWithData     bool
	corruptFirstResponse bool
}

type s3RangeBody struct {
	data             []byte
	fragmentSize     int
	terminalWithData bool
	terminalError    error
}

func (b *s3RangeBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(b.data) == 0 {
		return 0, b.terminalError
	}
	if len(p) > b.fragmentSize {
		p = p[:b.fragmentSize]
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	if len(b.data) == 0 && b.terminalWithData {
		return n, b.terminalError
	}
	return n, nil
}

func (*s3RangeBody) Close() error { return nil }

// Mock only the S3 boundary; Folder.ReadObject constructs the production
// RangeReader and ContentLengthValidator.
type s3RangeAPI struct {
	s3store.API
	data     []byte
	behavior s3RangeReadBehavior
	requests int
}

func (a *s3RangeAPI) GetObject(_ context.Context, in *awss3.GetObjectInput,
	_ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	a.requests++
	var start int
	if in.Range != nil {
		if _, err := fmt.Sscanf(*in.Range, "bytes=%d-", &start); err != nil {
			return nil, err
		}
	}
	if start < 0 || start >= len(a.data) {
		return nil, fmt.Errorf("invalid resume offset %d", start)
	}
	data := a.data[start:]
	terminalError := io.EOF
	if a.behavior.corruptFirstResponse && a.requests == 1 {
		// Model an interrupted response whose server appends an error message
		// before the connection fails. The retry must replace this suffix.
		data = append(append([]byte(nil), data[:18000]...), bytes.Repeat([]byte("X"), 300)...)
		terminalError = io.ErrUnexpectedEOF
	}
	out := &awss3.GetObjectOutput{
		Body: &s3RangeBody{
			data:             data,
			fragmentSize:     a.behavior.fragmentSize,
			terminalWithData: a.behavior.terminalWithData,
			terminalError:    terminalError,
		},
		ContentLength: aws.Int64(int64(len(a.data) - start)),
		ETag:          aws.String("fixture-etag"),
	}
	if in.Range != nil {
		out.ContentRange = aws.String(fmt.Sprintf("bytes %d-%d/%d", start, len(a.data)-1, len(a.data)))
	}
	return out, nil
}

func newS3RangeObject(t *testing.T, data []byte, behavior s3RangeReadBehavior) (io.ReadCloser, *s3RangeAPI) {
	t.Helper()
	api := &s3RangeAPI{data: data, behavior: behavior}
	folder := s3store.NewFolder(api, nil, "", &s3store.Config{
		Bucket: "fixture", RangeBatchEnabled: true, RangeMaxRetries: 2,
	})
	reader, err := folder.ReadObject(t.Context(), "fixture.xbstream")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	return reader, api
}

type rangeArchiveFixture struct {
	name       string
	archive    []byte
	files      map[string][]byte
	decompress bool
}

func writeRangeArchiveChunk(b *bytes.Buffer, path string, kind ChunkType,
	offset uint64, data []byte, sparse []SparseChunk) {
	b.Write(chunkMagic)
	b.WriteByte(0)
	b.WriteByte(byte(kind))
	b.Write(binary.LittleEndian.AppendUint32(nil, uint32(len(path))))
	b.WriteString(path)
	if kind == ChunkTypeEOF {
		return
	}
	var sparseBytes []byte
	if kind == ChunkTypeSparse {
		b.Write(binary.LittleEndian.AppendUint32(nil, uint32(len(sparse))))
		for _, extent := range sparse {
			sparseBytes = binary.LittleEndian.AppendUint32(sparseBytes, extent.SkipBytes)
			sparseBytes = binary.LittleEndian.AppendUint32(sparseBytes, extent.WriteBytes)
		}
	}
	b.Write(binary.LittleEndian.AppendUint64(nil, uint64(len(data))))
	b.Write(binary.LittleEndian.AppendUint64(nil, offset))
	checksum := crc32.Update(crc32.ChecksumIEEE(sparseBytes), crc32.IEEETable, data)
	b.Write(binary.LittleEndian.AppendUint32(nil, checksum))
	b.Write(sparseBytes)
	b.Write(data)
}

func newRangeArchiveFixtures(t *testing.T) []rangeArchiveFixture {
	t.Helper()
	// Incompressible data keeps the compressed objects large enough to fail
	// after 18000 bytes while also testing multiple XBStream payload chunks.
	data := make([]byte, 100000)
	_, err := rand.New(rand.NewSource(126)).Read(data)
	require.NoError(t, err)
	var plain bytes.Buffer
	for start := 0; start < len(data); start += 12000 {
		end := min(start+12000, len(data))
		writeRangeArchiveChunk(&plain, "large.data", ChunkTypePayload, uint64(start), data[start:end], nil)
	}
	writeRangeArchiveChunk(&plain, "large.data", ChunkTypeEOF, 0, nil, nil)
	writeRangeArchiveChunk(&plain, "empty.data", ChunkTypeEOF, 0, nil, nil)
	writeRangeArchiveChunk(&plain, "tiny.data", ChunkTypePayload, 0, []byte("last file\n"), nil)
	writeRangeArchiveChunk(&plain, "tiny.data", ChunkTypeEOF, 0, nil, nil)
	fixtures := []rangeArchiveFixture{{
		name: "plain-multifile", archive: plain.Bytes(),
		files: map[string][]byte{"large.data": data, "empty.data": {}, "tiny.data": []byte("last file\n")},
	}}

	var sparse bytes.Buffer
	extents := []SparseChunk{{SkipBytes: 4096, WriteBytes: 50000}, {SkipBytes: 4096, WriteBytes: 50000}}
	writeRangeArchiveChunk(&sparse, "sparse.data", ChunkTypeSparse, 0, data, extents)
	writeRangeArchiveChunk(&sparse, "sparse.data", ChunkTypeEOF, 0, nil, nil)
	sparseWant := make([]byte, len(data)+8192)
	copy(sparseWant[4096:], data[:50000])
	copy(sparseWant[8192+50000:], data[50000:])
	fixtures = append(fixtures, rangeArchiveFixture{
		name: "sparse", archive: sparse.Bytes(), files: map[string][]byte{"sparse.data": sparseWant},
	})

	for _, codec := range []compression.Compressor{lz4.Compressor{}, zstd.Compressor{}} {
		var compressed, archive bytes.Buffer
		writer := codec.NewWriter(&compressed)
		_, err := writer.Write(data)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		compressedData := compressed.Bytes()
		path := "compressed.data." + codec.FileExtension()
		for start := 0; start < len(compressedData); start += 11000 {
			end := min(start+11000, len(compressedData))
			writeRangeArchiveChunk(&archive, path, ChunkTypePayload, uint64(start), compressedData[start:end], nil)
		}
		writeRangeArchiveChunk(&archive, path, ChunkTypeEOF, 0, nil, nil)
		fixtures = append(fixtures, rangeArchiveFixture{
			name: codec.FileExtension(), archive: archive.Bytes(),
			files: map[string][]byte{"compressed.data": data}, decompress: true,
		})
	}
	return fixtures
}

func checkRangeArchiveExtraction(t *testing.T, fixture rangeArchiveFixture, behavior s3RangeReadBehavior) {
	t.Helper()
	// Check framing and payload checksums first. A corrupted test stream then
	// fails here instead of reaching sinks that log.Fatal on malformed input.
	reader, api := newS3RangeObject(t, fixture.archive, behavior)
	parser := NewReader(reader, true)
	fileEOFs := make(map[string]bool)
	for {
		chunk, err := parser.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "parse chunk")
		if chunk.Type == ChunkTypeEOF {
			require.False(t, fileEOFs[chunk.Path], "duplicate EOF for %s", chunk.Path)
			fileEOFs[chunk.Path] = true
			continue
		}
		n, err := io.Copy(io.Discard, chunk)
		require.NoError(t, err, "read payload")
		require.EqualValues(t, chunk.PayloadLen, n)
		require.NoError(t, chunk.ValidateCheckSum())
	}
	require.Len(t, fileEOFs, len(fixture.files))
	for path := range fixture.files {
		if fixture.decompress {
			path += "." + fixture.name
		}
		require.True(t, fileEOFs[path], "missing EOF for %s", path)
	}
	wantRequests := 1
	if behavior.corruptFirstResponse {
		wantRequests = 2
	}
	require.Equal(t, wantRequests, api.requests)

	// Exercise the real sparse/decompression sinks and their completion waits.
	reader, api = newS3RangeObject(t, fixture.archive, behavior)
	destination := t.TempDir()
	BackupSink(NewReader(reader, false), destination, fixture.decompress)
	for path, want := range fixture.files {
		got, err := os.ReadFile(filepath.Join(destination, path))
		require.NoError(t, err)
		require.Equal(t, want, got, "extracted file %s", path)
	}
	require.Equal(t, wantRequests, api.requests)
}

func TestS3RangeReaderXBStreamHealthy(t *testing.T) {
	for _, fixture := range newRangeArchiveFixtures(t) {
		for _, fragmentSize := range []int{1, 7, 4097, 32768} {
			for _, terminalWithData := range []bool{false, true} {
				name := fmt.Sprintf("%s/fragment%d/terminalWithData%t", fixture.name, fragmentSize, terminalWithData)
				t.Run(name, func(t *testing.T) {
					checkRangeArchiveExtraction(t, fixture, s3RangeReadBehavior{
						fragmentSize: fragmentSize, terminalWithData: terminalWithData,
					})
				})
			}
		}
	}
}

func TestS3RangeReaderXBStreamRetry(t *testing.T) {
	for _, fixture := range newRangeArchiveFixtures(t) {
		for _, fragmentSize := range []int{7, 4097, 32768} {
			for _, terminalWithData := range []bool{false, true} {
				name := fmt.Sprintf("%s/fragment%d/terminalWithData%t", fixture.name, fragmentSize, terminalWithData)
				t.Run(name, func(t *testing.T) {
					checkRangeArchiveExtraction(t, fixture, s3RangeReadBehavior{
						fragmentSize: fragmentSize, terminalWithData: terminalWithData, corruptFirstResponse: true,
					})
				})
			}
		}
	}
}
