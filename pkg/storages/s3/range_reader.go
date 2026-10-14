package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wal-g/tracelog"
)

const (
	minRetryDelay = 30 * time.Millisecond
	maxRetryDelay = 300 * time.Second

	// Keep a suffix private so error text appended to a failed response can
	// be discarded before the next ranged request. This is a bounded guard,
	// not a substitute for object integrity verification.
	rangeRetryTailSize = 4 * 1024
	rangeReadSize      = 32 * 1024
	rangeMaxEmptyReads = 100
)

var rangeReaderCounter atomic.Uint64

type RangeReader struct {
	ctx            context.Context //nolint:containedctx // io.Reader has no context parameter
	lastBody       io.ReadCloser
	folder         *Folder
	input          s3.GetObjectInput
	maxRetries     int
	reconnects     int
	storageCursor  int64 // bytes committed to the caller buffer, excluding private read-ahead
	expectedLength int64
	pendingError   error
	terminalError  error
	closed         bool
	logDebugID     uint64
	wait           func(context.Context, time.Duration) error
	buffer         [rangeRetryTailSize + rangeReadSize]byte
	bufferOffset   int
	buffered       int
}

func NewRangeReader(ctx context.Context, body io.ReadCloser, objectPath string, retriesCount int, folder *Folder) *RangeReader {
	return newRangeReader(ctx, folder.getObjectInput(objectPath), &s3.GetObjectOutput{Body: body}, retriesCount, folder)
}

func newRangeReader(ctx context.Context, input *s3.GetObjectInput, object *s3.GetObjectOutput,
	retriesCount int, folder *Folder) *RangeReader {
	r := &RangeReader{ctx: ctx, lastBody: object.Body, folder: folder, input: *input,
		maxRetries: max(0, retriesCount), expectedLength: -1,
		logDebugID: rangeReaderCounter.Add(1), wait: waitForRangeRetry}
	if object.ContentLength != nil {
		r.expectedLength = *object.ContentLength
	}
	if object.VersionId != nil {
		r.input.VersionId = object.VersionId
	}
	if object.ETag != nil {
		r.input.IfMatch = object.ETag
	}
	return r
}

// Read fills p with safe bytes unless the object ends or an error occurs.
// A partial final read returns its byte count together with EOF or the error.
func (r *RangeReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	n, emptyReads := 0, 0
	for {
		if err := r.ctx.Err(); err != nil {
			return n, err
		}
		// Coalesce safe bytes across body reads and retries so retaining the
		// tail does not expose fragmented fields to existing callers.
		n += r.readBuffered(p[n:])
		if n == len(p) {
			return n, nil
		}
		if r.terminalError != nil {
			return n, r.terminalError
		}
		if r.pendingError != nil || r.lastBody == nil {
			if err := r.reconnect(); err != nil {
				r.terminalError = err
				return n, err
			}
		}
		if read, err := r.readAhead(); read == 0 && err == nil {
			emptyReads++
			if emptyReads >= rangeMaxEmptyReads {
				r.terminalError = io.ErrNoProgress
				return n, r.terminalError
			}
		} else {
			emptyReads = 0
		}
	}
}

func (r *RangeReader) readBuffered(p []byte) int {
	available := r.buffered - r.bufferOffset
	if r.terminalError != io.EOF {
		available -= rangeRetryTailSize
	}
	if available <= 0 {
		return 0
	}
	n := copy(p, r.buffer[r.bufferOffset:r.bufferOffset+available])
	r.bufferOffset += n
	r.storageCursor += int64(n)
	return n
}

func (r *RangeReader) readAhead() (int, error) {
	// Drain all but the retained suffix before reading more. Compact only
	// here, so small caller buffers do not repeatedly copy the whole buffer.
	r.buffered = copy(r.buffer[:], r.buffer[r.bufferOffset:r.buffered])
	r.bufferOffset = 0
	n, err := r.lastBody.Read(r.buffer[r.buffered:])
	r.buffered += n
	err = r.checkReadLength(err)
	if err == nil {
		return n, nil
	}
	if err == io.EOF {
		// Only a clean EOF releases the retained suffix.
		r.terminalError = err
		return n, err
	}
	// The retained suffix and bytes from the failed Read have not reached
	// the caller. Discard them and resume from the delivered offset.
	r.buffered = 0
	if r.folder.IsRetryableError(err) {
		r.pendingError = err
	} else {
		r.terminalError = err
	}
	return n, err
}

func (r *RangeReader) checkReadLength(err error) error {
	readCursor := r.storageCursor + int64(r.buffered-r.bufferOffset)
	if r.expectedLength >= 0 {
		if readCursor > r.expectedLength {
			return errors.New("S3 range response exceeds the original content length")
		}
		if err == io.EOF && readCursor < r.expectedLength {
			return io.ErrUnexpectedEOF
		}
	}
	return err
}

func (r *RangeReader) reconnect() error {
	for {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		if r.pendingError != nil {
			if r.reconnects >= r.maxRetries {
				return fmt.Errorf("S3 range recovery exhausted after %d reconnections: %w", r.reconnects, r.pendingError)
			}
			delay := r.getIncrSleep(r.reconnects)
			r.closeBody()
			tracelog.DebugLogger.Printf("RangeReader [%d]: reconnect %d/%d after transient read failure in %s",
				r.logDebugID, r.reconnects+1, r.maxRetries, delay)
			if err := r.wait(r.ctx, delay); err != nil {
				return err
			}
			r.reconnects++
		}
		input := r.input
		input.Range = aws.String(fmt.Sprintf("bytes=%d-", r.storageCursor))
		object, err := r.folder.s3API.GetObject(r.ctx, &input)
		if err != nil {
			if !r.folder.IsRetryableError(err) {
				return err
			}
			r.pendingError = err
			continue
		}
		if err := r.validateRange(object); err != nil {
			if object.Body != nil {
				_ = object.Body.Close()
			}
			return err
		}
		r.lastBody = object.Body
		r.pendingError = nil
		return nil
	}
}

func (r *RangeReader) validateRange(object *s3.GetObjectOutput) error {
	if object.Body == nil {
		return errors.New("S3 range response has no body")
	}
	if r.input.IfMatch != nil && object.ETag != nil && *r.input.IfMatch != *object.ETag {
		return errors.New("S3 object ETag changed during range recovery")
	}
	if r.input.VersionId != nil && object.VersionId != nil && *r.input.VersionId != *object.VersionId {
		return errors.New("S3 object version changed during range recovery")
	}
	if object.ContentRange == nil {
		if r.storageCursor != 0 {
			return errors.New("S3 range response is missing Content-Range")
		}
		if object.ContentLength != nil {
			if r.expectedLength >= 0 && r.expectedLength != *object.ContentLength {
				return errors.New("S3 object length changed during range recovery")
			}
			r.expectedLength = *object.ContentLength
		}
		return nil
	}
	return r.validateContentRange(object)
}

func (r *RangeReader) validateContentRange(object *s3.GetObjectOutput) error {
	var start, end, total int64
	n, err := fmt.Sscanf(*object.ContentRange, "bytes %d-%d/%d", &start, &end, &total)
	if err != nil || n != 3 || start != r.storageCursor || end < start || total <= end ||
		*object.ContentRange != fmt.Sprintf("bytes %d-%d/%d", start, end, total) ||
		(r.expectedLength >= 0 && total != r.expectedLength) ||
		(object.ContentLength != nil && *object.ContentLength != end-start+1) {
		return errors.New("S3 range response has inconsistent Content-Range or length")
	}
	r.expectedLength = total
	return nil
}

func (r *RangeReader) getIncrSleep(retryCount int) time.Duration {
	delay := minRetryDelay
	for i := 0; i < retryCount && delay < maxRetryDelay/2; i++ {
		delay = min(delay*2, maxRetryDelay/2)
	}
	return delay + time.Duration(rand.Int64N(int64(delay)))
}

func waitForRangeRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func (r *RangeReader) closeBody() {
	if r.lastBody != nil {
		if err := r.lastBody.Close(); err != nil {
			tracelog.DebugLogger.Printf("RangeReader [%d]: failed to close interrupted response body", r.logDebugID)
		}
		r.lastBody = nil
	}
}

func (r *RangeReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.lastBody == nil {
		return nil
	}
	err := r.lastBody.Close()
	r.lastBody = nil
	return err
}
