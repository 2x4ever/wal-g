package internal_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/compression/lzma"
	"github.com/wal-g/wal-g/internal/compression/none"
	"github.com/wal-g/wal-g/internal/compression/zstd"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	s3store "github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type s3RestoreBody struct {
	data     []byte
	fragment int
	together bool
	end      error
}

func (b *s3RestoreBody) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, b.end
	}
	n := copy(p[:min(len(p), b.fragment)], b.data)
	b.data = b.data[n:]
	if len(b.data) == 0 && b.together {
		return n, b.end
	}
	return n, nil
}
func (*s3RestoreBody) Close() error { return nil }

type s3RestoreAPI struct {
	s3store.API
	data           []byte
	fragment       int
	together, fail bool
	gets           int
	retries        *atomic.Int64
}

func (a *s3RestoreAPI) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	a.gets++
	start := 0
	if in.Range != nil {
		if _, err := fmt.Sscanf(*in.Range, "bytes=%d-", &start); err != nil {
			return nil, err
		}
		a.retries.Add(1)
	}
	if start < 0 || start > len(a.data) {
		return nil, fmt.Errorf("invalid resume %d", start)
	}
	data := a.data[start:]
	end := io.EOF
	if a.fail && a.gets == 1 && len(data) > 19000 {
		data = append(append([]byte(nil), data[:18000]...), bytes.Repeat([]byte("!"), 300)...)
		end = io.ErrUnexpectedEOF
	}
	out := &awss3.GetObjectOutput{
		Body:          &s3RestoreBody{data: data, fragment: a.fragment, together: a.together, end: end},
		ContentLength: aws.Int64(int64(len(a.data) - start)),
		ETag:          aws.String("test-etag"),
	}
	if in.Range != nil {
		out.ContentRange = aws.String(fmt.Sprintf("bytes %d-%d/%d", start, len(a.data)-1, len(a.data)))
	}
	return out, nil
}

type s3RestoreFolder struct {
	storage.Folder
	fragment       int
	together, fail bool
	retries        *atomic.Int64
}

func (f *s3RestoreFolder) ReadObject(ctx context.Context, name string) (io.ReadCloser, error) {
	r, err := f.Folder.ReadObject(ctx, name)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(r)
	closeErr := r.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	api := &s3RestoreAPI{data: data, fragment: f.fragment, together: f.together, fail: f.fail, retries: f.retries}
	folder := s3store.NewFolder(api, nil, "", &s3store.Config{Bucket: "test", RangeBatchEnabled: true, RangeMaxRetries: 2})
	return folder.ReadObject(ctx, name)
}
func (f *s3RestoreFolder) GetSubFolder(name string) storage.Folder {
	return &s3RestoreFolder{f.Folder.GetSubFolder(name), f.fragment, f.together, f.fail, f.retries}
}
func s3RestoreNewFolder(fragment int, together, fail bool) *s3RestoreFolder {
	return &s3RestoreFolder{memory.NewFolder("", memory.NewKVS()), fragment, together, fail, new(atomic.Int64)}
}

// gzip is supported for decompression; this adapter is only fixture generation.
type s3RestoreGzip struct{}

func (s3RestoreGzip) FileExtension() string                               { return "gz" }
func (s3RestoreGzip) NewWriter(w io.Writer) ioextensions.WriteFlushCloser { return gzip.NewWriter(w) }
func s3RestoreCodecs() []compression.Compressor {
	return []compression.Compressor{none.Compressor{}, lz4.Compressor{}, lzma.Compressor{}, zstd.Compressor{}, s3RestoreGzip{}}
}
func s3RestoreData(n int) []byte {
	p := make([]byte, n)
	_, _ = rand.New(rand.NewSource(73)).Read(p)
	return p
}
func s3RestoreCompress(t *testing.T, data []byte, c compression.Compressor) []byte {
	t.Helper()
	var out bytes.Buffer
	w := c.NewWriter(&out)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// Run the real restore consumers against fragmented and interrupted S3 bodies.
// These are component integration tests; no database server or cloud is needed.
func s3RestoreRun(t *testing.T, fail bool, fn func(*testing.T, compression.Compressor, int, bool, bool)) {
	t.Helper()
	for _, c := range s3RestoreCodecs() {
		if compression.FindDecompressor(c.FileExtension()) == nil {
			continue // Some platforms do not register gzip decompression.
		}
		for _, chunk := range []int{1, 7, 4097, 32768} {
			if fail && chunk != 4097 {
				continue
			}
			for _, together := range []bool{false, true} {
				t.Run(fmt.Sprintf("codec=%s/chunk=%d/together=%t", c.FileExtension(), chunk, together), func(t *testing.T) {
					fn(t, c, chunk, together, fail)
				})
			}
		}
	}
}
func s3RestoreTar(t *testing.T) ([]byte, map[string][]byte) {
	t.Helper()
	files := map[string][]byte{"large.bin": s3RestoreData(150003), "empty.bin": {}, "tiny.bin": []byte("last bytes")}
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	for _, name := range []string{"large.bin", "empty.bin", "tiny.bin"} {
		data := files[name]
		if err := w.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes(), files
}
func s3RestoreCheckRetries(t *testing.T, f *s3RestoreFolder, fail bool) {
	t.Helper()
	if fail && f.retries.Load() == 0 {
		t.Fatal("fault did not trigger retry")
	}
	if !fail && f.retries.Load() != 0 {
		t.Fatal("healthy download unexpectedly retried")
	}
}
func s3RestoreTarCase(t *testing.T, c compression.Compressor, chunk int, together, fail bool) {
	f := s3RestoreNewFolder(chunk, together, fail)
	data, want := s3RestoreTar(t)
	name := "backup/tar_partitions/part.tar"
	if c.FileExtension() != "" {
		name += "." + c.FileExtension()
	}
	if err := f.PutObject(t.Context(), name, bytes.NewReader(s3RestoreCompress(t, data, c))); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	uploader := internal.NewRegularUploader(c, f)
	if err := internal.CreateConcurrentDownloader(uploader, nil).Download(t.Context(), "backup", dest, nil); err != nil {
		t.Fatal(err)
	}
	for name, data := range want {
		got, err := os.ReadFile(filepath.Join(dest, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, got) {
			t.Fatalf("wrong extracted bytes for %s: %d/%d", name, len(got), len(data))
		}
	}
	s3RestoreCheckRetries(t, f, fail)
}
func TestS3RangeReaderRestoreTarHealthy(t *testing.T) { s3RestoreRun(t, false, s3RestoreTarCase) }
func TestS3RangeReaderRestoreTarRetry(t *testing.T)   { s3RestoreRun(t, true, s3RestoreTarCase) }
func s3RestoreStreamCase(t *testing.T, c compression.Compressor, chunk int, together, fail bool) {
	for _, maxFileSize := range []int{0, 40000} {
		t.Run(fmt.Sprintf("maxFile=%d", maxFileSize), func(t *testing.T) {
			f := s3RestoreNewFolder(chunk, together, fail)
			data := s3RestoreData(300019)
			uploader := internal.NewSplitStreamUploader(internal.NewRegularUploader(c, f), 3, 1009, maxFileSize)
			name, err := uploader.PushStream(t.Context(), bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			writer := newTestWriter()
			backup := internal.Backup{Name: name, Folder: f}
			// Exercise metadata dispatch and partition listing as real restore does.
			fetch, err := internal.GetBackupStreamFetcher(t.Context(), backup)
			if err != nil {
				t.Fatal(err)
			}
			if err := fetch(t.Context(), backup, writer); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, writer.Result) {
				t.Fatalf("merged data differs: got %d expected %d", len(writer.Result), len(data))
			}
			s3RestoreCheckRetries(t, f, fail)
		})
	}
}
func TestS3RangeReaderRestoreStreamHealthy(t *testing.T) { s3RestoreRun(t, false, s3RestoreStreamCase) }
func TestS3RangeReaderRestoreStreamRetry(t *testing.T)   { s3RestoreRun(t, true, s3RestoreStreamCase) }
func s3RestoreOpenPGPCase(t *testing.T, c compression.Compressor, chunk int, together, fail bool) {
	data := s3RestoreData(100003)
	crypter := openpgp.CrypterFromKeyPath("../test/testdata/waleGpgKey", noPassphrase)
	encrypted := internal.CompressAndEncrypt(t.Context(), bytes.NewReader(data), c, crypter)
	object, err := io.ReadAll(encrypted)
	if err != nil {
		t.Fatal(err)
	}
	f := s3RestoreNewFolder(chunk, together, fail)
	if err = f.PutObject(t.Context(), "data", bytes.NewReader(object)); err != nil {
		t.Fatal(err)
	}
	raw, err := f.ReadObject(t.Context(), "data")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	name := "data.tar"
	if c.FileExtension() != "" {
		name += "." + c.FileExtension()
	}
	r, err := internal.DecryptAndDecompressTar(t.Context(), raw, name, crypter)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, got) {
		t.Fatalf("decrypt bytes differ: %d/%d", len(got), len(data))
	}
	s3RestoreCheckRetries(t, f, fail)
}
func TestS3RangeReaderRestoreOpenPGPHealthy(t *testing.T) {
	s3RestoreRun(t, false, s3RestoreOpenPGPCase)
}
func TestS3RangeReaderRestoreOpenPGPRetry(t *testing.T) { s3RestoreRun(t, true, s3RestoreOpenPGPCase) }
