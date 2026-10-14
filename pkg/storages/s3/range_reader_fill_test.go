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

// A caller requesting a complete field or block must not receive a short
// successful read merely because the underlying response was fragmented.
func TestRangeReadFillsCallerBuffer(t *testing.T) {
	for _, sourceChunk := range []int{1, 4097, 32768} {
		for _, callerChunk := range []int{184, 4096, 32768, 65536} {
			t.Run(fmt.Sprintf("source=%d/caller=%d", sourceChunk, callerChunk), func(t *testing.T) {
				original := rangeTailOriginal(100003)
				body := &rangeTailBody{steps: []rangeTailRead{{data: original, err: io.EOF}}, chunk: sourceChunk}
				r := NewRangeReader(t.Context(), body, "object", 0, NewFolder(nil, nil, "", &Config{}))
				defer r.Close()
				p := make([]byte, callerChunk)
				offset := 0
				for offset < len(original) {
					n, err := r.Read(p)
					want := min(callerChunk, len(original)-offset)
					require.Equal(t, want, n)
					require.Equal(t, original[offset:offset+want], string(p[:n]))
					if want == callerChunk {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, io.EOF)
					}
					offset += n
				}
				n, err := r.Read(p)
				require.Zero(t, n)
				require.ErrorIs(t, err, io.EOF)
			})
		}
	}
}

func TestRangeReadPreservesSafePrefixOnFailure(t *testing.T) {
	fatal := errors.New("AccessDenied")
	for _, test := range []struct {
		name    string
		readErr error
		retries int
		want    error
	}{
		{"body error", fatal, 0, fatal},
		{"exhausted retry", io.ErrUnexpectedEOF, 0, io.ErrUnexpectedEOF},
		{"reopen error", io.ErrUnexpectedEOF, 1, fatal},
	} {
		t.Run(test.name, func(t *testing.T) {
			initial := &rangeTailBody{steps: []rangeTailRead{
				{data: "0123456789" + strings.Repeat("!", 4096)},
				{data: "untrusted failed-read bytes", err: test.readErr},
			}}
			gets := 0
			folder := NewFolder(&rangeAPI{get: func(input *awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
				gets++
				require.Equal(t, "bytes=10-", aws.ToString(input.Range))
				return nil, fatal
			}}, nil, "", &Config{})
			r := NewRangeReader(t.Context(), initial, "object", test.retries, folder)
			r.wait = noRangeWait
			defer r.Close()
			p := make([]byte, 64)
			n, err := r.Read(p)
			require.ErrorIs(t, err, test.want)
			require.Equal(t, "0123456789", string(p[:n]), "private and failed-read bytes must not escape")
			require.Equal(t, test.retries, gets)
			n, err = r.Read(p)
			require.Zero(t, n)
			require.ErrorIs(t, err, test.want)
		})
	}
}

type fillReadFunc func([]byte) (int, error)

func (f fillReadFunc) Read(p []byte) (int, error) { return f(p) }

func TestRangeReadToleratesTemporaryEmptyReads(t *testing.T) {
	body := &rangeTailBody{steps: []rangeTailRead{{}, {}, {data: "abc", err: io.EOF}}}
	r := NewRangeReader(t.Context(), body, "object", 0, NewFolder(nil, nil, "", &Config{}))
	defer r.Close()
	p := make([]byte, 3)
	n, err := r.Read(p)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "abc", string(p[:n]))
}

func TestRangeReadStopsOnNoProgress(t *testing.T) {
	calls := 0
	body := io.NopCloser(fillReadFunc(func(p []byte) (int, error) {
		calls++
		if calls == 1 {
			return copy(p, "abc"+strings.Repeat("!", 4096)), nil
		}
		if calls > 1000 {
			return 0, errors.New("test safeguard: unbounded empty reads")
		}
		return 0, nil
	}))
	r := NewRangeReader(t.Context(), body, "object", 0, NewFolder(nil, nil, "", &Config{}))
	defer r.Close()
	p := make([]byte, 64)
	n, err := r.Read(p)
	require.ErrorIs(t, err, io.ErrNoProgress)
	require.Equal(t, "abc", string(p[:n]))
	require.Less(t, calls, 1000)
	previousCalls := calls
	n, err = r.Read(p)
	require.Zero(t, n)
	require.ErrorIs(t, err, io.ErrNoProgress)
	require.Equal(t, previousCalls, calls, "terminal no-progress failure must not read again")
}

func TestRangeReadCancellationWhileFilling(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	body := io.NopCloser(fillReadFunc(func(p []byte) (int, error) {
		calls++
		if calls == 1 {
			return copy(p, "abc"+strings.Repeat("!", 4096)), nil
		}
		cancel()
		return 0, nil
	}))
	r := NewRangeReader(ctx, body, "object", 0, NewFolder(nil, nil, "", &Config{}))
	defer r.Close()
	p := make([]byte, 64)
	n, err := r.Read(p)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, "abc", string(p[:n]))
	require.Equal(t, 2, calls)
}

func TestRangeReadFillsAcrossMultipleRetries(t *testing.T) {
	original := rangeTailOriginal(10000)
	initial := &rangeTailBody{steps: []rangeTailRead{
		{data: original[:5000] + strings.Repeat("!", 300)}, {err: io.ErrUnexpectedEOF},
	}}
	second := &rangeTailBody{steps: []rangeTailRead{
		{data: original[1204:7204] + strings.Repeat("?", 128)}, {err: io.ErrUnexpectedEOF},
	}, chunk: 137}
	gets := 0
	folder := NewFolder(&rangeAPI{get: func(input *awss3.GetObjectInput) (*awss3.GetObjectOutput, error) {
		gets++
		switch gets {
		case 1:
			require.Equal(t, "bytes=1204-", aws.ToString(input.Range))
			return rangeTailResponse(second, 1204, len(original)), nil
		case 2:
			require.Equal(t, "bytes=3236-", aws.ToString(input.Range))
			return rangeTailResponse(io.NopCloser(strings.NewReader(original[3236:])), 3236, len(original)), nil
		default:
			t.Fatal("unexpected extra retry")
			return nil, errors.New("unexpected extra retry")
		}
	}}, nil, "", &Config{})
	r := newRangeReader(t.Context(), folder.getObjectInput("object"),
		&awss3.GetObjectOutput{Body: initial, ContentLength: aws.Int64(10000)}, 2, folder)
	r.wait = noRangeWait
	defer r.Close()

	p := make([]byte, 8000)
	n, err := r.Read(p)
	require.NoError(t, err)
	require.Equal(t, 8000, n, "one Read must fill the requested buffer across both retries")
	require.Equal(t, original[:8000], string(p[:n]))
	require.Equal(t, 2, gets)
	n, err = r.Read(p)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, original[8000:], string(p[:n]))
}
