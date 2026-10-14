package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

type rangeTailRead struct {
	data string
	err  error
}

// Each step preserves the distinction between successful reads containing
// provider error text and bytes returned together with a read error.
type rangeTailBody struct {
	steps  []rangeTailRead
	chunk  int
	closed bool
}

func (b *rangeTailBody) Read(p []byte) (int, error) {
	if len(b.steps) == 0 {
		return 0, io.EOF
	}
	if b.chunk > 0 && len(p) > b.chunk {
		p = p[:b.chunk]
	}
	step := &b.steps[0]
	n := copy(p, step.data)
	step.data = step.data[n:]
	if step.data != "" {
		return n, nil
	}
	err := step.err
	b.steps = b.steps[1:]
	return n, err
}

func (b *rangeTailBody) Close() error {
	b.closed = true
	return nil
}

func rangeTailOriginal(size int) string {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*37 + i/251) % 256)
	}
	return string(data)
}

func rangeTailResponse(body io.ReadCloser, start, total int) *awss3.GetObjectOutput {
	return &awss3.GetObjectOutput{
		Body:          body,
		ContentRange:  aws.String(fmt.Sprintf("bytes %d-%d/%d", start, total-1, total)),
		ContentLength: aws.Int64(int64(total - start)),
	}
}

func readRangeTailChunks(r io.Reader, chunk int) (string, error) {
	var result strings.Builder
	buffer := make([]byte, chunk)
	for {
		n, err := r.Read(buffer)
		result.Write(buffer[:n])
		if err == io.EOF {
			return result.String(), nil
		}
		if err != nil {
			return result.String(), err
		}
	}
}

func TestRangeTailDiscardsProviderErrorText(t *testing.T) {
	for _, junkSize := range []int{300, 4096} {
		for _, readError := range []error{io.ErrUnexpectedEOF, io.EOF} {
			t.Run(fmt.Sprintf("junk=%d/error=%s", junkSize, readError), func(t *testing.T) {
				original := rangeTailOriginal(32768)
				prefix := original[:12288]
				initial := &rangeTailBody{steps: []rangeTailRead{
					{data: prefix + strings.Repeat("!", junkSize)}, {err: readError},
				}, chunk: 137}
				wantStart := len(prefix) + junkSize - 4096
				gets := 0
				resumed := &rangeTailBody{steps: []rangeTailRead{{data: original[wantStart:], err: io.EOF}}, chunk: 251}
				api := &rangeAPI{get: func(in *awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
					gets++
					require.Equal(t, fmt.Sprintf("bytes=%d-", wantStart), aws.ToString(in.Range))
					require.True(t, initial.closed)
					return rangeTailResponse(resumed, wantStart, len(original)), nil
				}}
				folder := NewFolder(api, nil, "", &Config{})
				r := newRangeReader(t.Context(), folder.getObjectInput("object"),
					&awss3.GetObjectOutput{Body: initial, ContentLength: aws.Int64(int64(len(original)))}, 1, folder)
				r.wait = noRangeWait
				defer r.Close()
				actual, err := readRangeTailChunks(r, 17)
				require.NoError(t, err)
				require.True(t, original == actual, "provider text must not replace original object bytes")
				require.Equal(t, 1, gets)
			})
		}
	}
}

func TestRangeTailDiscardsBytesReturnedWithReadError(t *testing.T) {
	original := rangeTailOriginal(32768)
	initial := &rangeTailBody{steps: []rangeTailRead{
		{data: original[:12288] + strings.Repeat("!", 300)}, {data: "failed read bytes", err: io.ErrUnexpectedEOF},
	}}
	firstStart := 12288 + 300 - 4096
	secondStart := firstStart + 6000 + 128 - 4096
	second := &rangeTailBody{steps: []rangeTailRead{
		{data: original[firstStart:firstStart+6000] + strings.Repeat("?", 128)},
		{data: "more failed read bytes", err: io.ErrUnexpectedEOF},
	}, chunk: 173}
	gets := 0
	api := &rangeAPI{get: func(in *awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
		gets++
		start := firstStart
		var body io.ReadCloser = second
		if gets == 2 {
			start = secondStart
			body = &rangeTailBody{steps: []rangeTailRead{{data: original[start:], err: io.EOF}}, chunk: 233}
		}
		require.LessOrEqual(t, gets, 2)
		require.Equal(t, fmt.Sprintf("bytes=%d-", start), aws.ToString(in.Range))
		return rangeTailResponse(body, start, len(original)), nil
	}}
	folder := NewFolder(api, nil, "", &Config{})
	r := newRangeReader(t.Context(), folder.getObjectInput("object"),
		&awss3.GetObjectOutput{Body: initial, ContentLength: aws.Int64(int64(len(original)))}, 2, folder)
	r.wait = noRangeWait
	defer r.Close()
	actual, err := readRangeTailChunks(r, 31)
	require.NoError(t, err)
	require.True(t, original == actual, "neither failed Read's bytes nor either held corrupt suffix may escape")
	require.Equal(t, 2, gets)
	require.True(t, initial.closed)
	require.True(t, second.closed)
}

func TestRangeTailCleanEOFBoundaryMatrix(t *testing.T) {
	for _, size := range []int{0, 1, 4095, 4096, 4097, 32768, 36865} {
		for _, sourceChunk := range []int{1, 4096, 32768} {
			for _, callerChunk := range []int{1, 17, 4096, 8192} {
				t.Run(fmt.Sprintf("size=%d/source=%d/caller=%d", size, sourceChunk, callerChunk), func(t *testing.T) {
					original := rangeTailOriginal(size)
					body := &rangeTailBody{steps: []rangeTailRead{{data: original, err: io.EOF}}, chunk: sourceChunk}
					folder := NewFolder(&rangeAPI{get: func(*awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
						t.Fatal("clean EOF must not reconnect")
						return nil, nil
					}}, nil, "", &Config{})
					r := newRangeReader(t.Context(), folder.getObjectInput("object"),
						&awss3.GetObjectOutput{Body: body, ContentLength: aws.Int64(int64(size))}, 1, folder)
					defer r.Close()
					n, err := r.Read(nil)
					require.Zero(t, n)
					require.NoError(t, err)
					actual, err := readRangeTailChunks(r, callerChunk)
					require.NoError(t, err)
					require.True(t, original == actual, "clean stream must contain no duplicates or missing tail bytes")
					n, err = r.Read(make([]byte, 1))
					require.Zero(t, n)
					require.ErrorIs(t, err, io.EOF)
				})
			}
		}
	}
}

func TestRangeTailFailureDoesNotReleasePrivateBytes(t *testing.T) {
	fatal := errors.New("AccessDenied")
	for _, readError := range []error{io.ErrUnexpectedEOF, fatal} {
		t.Run(readError.Error(), func(t *testing.T) {
			body := &rangeTailBody{steps: []rangeTailRead{
				{data: strings.Repeat("a", 4096)}, {data: "untrusted final bytes", err: readError},
			}}
			gets := 0
			folder := NewFolder(&rangeAPI{get: func(*awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
				gets++
				return nil, errors.New("unexpected GET")
			}}, nil, "", &Config{})
			r := newRangeReader(t.Context(), folder.getObjectInput("object"),
				&awss3.GetObjectOutput{Body: body, ContentLength: aws.Int64(8192)}, 0, folder)
			r.wait = noRangeWait
			actual, err := io.ReadAll(r)
			require.ErrorIs(t, err, readError)
			require.Empty(t, actual, "terminal errors must not flush buffered or failed-read bytes")
			require.Zero(t, gets)
			n, again := r.Read(make([]byte, 17))
			require.Zero(t, n)
			require.ErrorIs(t, again, readError)
			require.Zero(t, gets)
			require.NoError(t, r.Close())
			require.True(t, body.closed)
		})
	}
}

func TestRangeTailTerminalGetFailureDoesNotReleasePrivateBytes(t *testing.T) {
	fatal := errors.New("AccessDenied")
	initial := &rangeTailBody{steps: []rangeTailRead{{data: strings.Repeat("a", 4096)}, {err: io.ErrUnexpectedEOF}}}
	gets := 0
	folder := NewFolder(&rangeAPI{get: func(in *awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
		gets++
		require.Equal(t, "bytes=0-", aws.ToString(in.Range))
		return nil, fatal
	}}, nil, "", &Config{})
	r := NewRangeReader(t.Context(), initial, "object", 2, folder)
	r.wait = noRangeWait
	actual, err := io.ReadAll(r)
	require.ErrorIs(t, err, fatal)
	require.Empty(t, actual)
	require.Equal(t, 1, gets)
	require.True(t, initial.closed)
	require.NoError(t, r.Close())
}

func TestRangeTailCancellationAndCloseWhileBuffered(t *testing.T) {
	for _, action := range []string{"cancel", "close"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			body := &rangeTailBody{steps: []rangeTailRead{{data: strings.Repeat("a", 4097)}}}
			folder := NewFolder(&rangeAPI{get: func(*awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
				t.Fatal("buffered cancellation or Close must not GET")
				return nil, nil
			}}, nil, "", &Config{})
			r := NewRangeReader(ctx, body, "object", 2, folder)
			defer r.Close()
			p := make([]byte, 1)
			n, err := r.Read(p)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.Equal(t, byte('a'), p[0])
			wantError := error(context.Canceled)
			if action == "cancel" {
				cancel()
			} else {
				require.NoError(t, r.Close())
				wantError = io.ErrClosedPipe
			}
			n, err = r.Read(make([]byte, 8192))
			require.Zero(t, n)
			require.ErrorIs(t, err, wantError)
			require.NoError(t, r.Close())
			require.True(t, body.closed)
		})
	}
}
