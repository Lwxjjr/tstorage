package tstorage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
)

// diskWAL 包含多个段文件。一个段负责一个分区。
// 它们可以很容易地排序，因为它们使用创建的时间戳命名。
// 宏观布局如下：
/*
  .wal/
  ├── 0
  └── 1
*/
type diskWAL struct {
	dir          string
	bufferedSize int
	// 活动段的缓冲写入器
	w *bufio.Writer
	// 活动段的文件描述符
	fd    *os.File
	index uint32
	mu    sync.Mutex
}

func newDiskWAL(dir string, bufferedSize int) (wal, error) {
	if err := os.MkdirAll(dir, fs.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to make WAL dir: %w", err)
	}
	w := &diskWAL{
		dir:          dir,
		bufferedSize: bufferedSize,
	}
	f, err := w.createSegmentFile(dir)
	if err != nil {
		return nil, err
	}
	w.fd = f
	w.w = bufio.NewWriterSize(f, bufferedSize)

	return w, nil
}

// append 通过它拥有的文件描述符将给定的条目追加到文件的末尾。
func (w *diskWAL) append(op walOperation, rows []Row) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch op {
	case operationInsert:
		for _, row := range rows {
			// 写入操作类型
			if err := w.w.WriteByte(byte(op)); err != nil {
				return fmt.Errorf("failed to write operation: %w", err)
			}
			name := marshalMetricName(row.Metric, row.Labels)
			// 写入指标名称的长度
			lBuf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(lBuf, uint64(len(name)))
			if _, err := w.w.Write(lBuf[:n]); err != nil {
				return fmt.Errorf("failed to write the length of the metric name: %w", err)
			}
			// 写入指标名称
			if _, err := w.w.WriteString(name); err != nil {
				return fmt.Errorf("failed to write the metric name: %w", err)
			}
			// 写入时间戳
			tsBuf := make([]byte, binary.MaxVarintLen64)
			n = binary.PutVarint(tsBuf, row.DataPoint.Timestamp)
			if _, err := w.w.Write(tsBuf[:n]); err != nil {
				return fmt.Errorf("failed to write the timestamp: %w", err)
			}
			// 写入值
			vBuf := make([]byte, binary.MaxVarintLen64)
			n = binary.PutUvarint(vBuf, math.Float64bits(row.DataPoint.Value))
			if _, err := w.w.Write(vBuf[:n]); err != nil {
				return fmt.Errorf("failed to write the value: %w", err)
			}
		}
	default:
		return fmt.Errorf("unknown operation %v given", op)
	}
	if w.bufferedSize == 0 {
		return w.flush()
	}

	return nil
}

// flush 将所有缓冲的条目刷新到底层文件。
func (w *diskWAL) flush() error {
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffered-data into the underlying WAL file: %w", err)
	}
	return nil
}

// punctuate 设置边界并创建一个新的段。
func (w *diskWAL) punctuate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.flush(); err != nil {
		return err
	}
	if err := w.fd.Close(); err != nil {
		return err
	}
	f, err := w.createSegmentFile(w.dir)
	if err != nil {
		return err
	}
	w.fd = f
	w.w = bufio.NewWriterSize(f, w.bufferedSize)
	return nil
}

// truncateOldest 仅删除最旧的段。
func (w *diskWAL) removeOldest() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no segment found")
	}
	return os.RemoveAll(filepath.Join(w.dir, files[0].Name()))
}

// removeAll 删除所有段文件。
func (w *diskWAL) removeAll() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fd.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(w.dir); err != nil {
		return fmt.Errorf("failed to remove files under %q: %w", w.dir, err)
	}
	return os.MkdirAll(w.dir, fs.ModePerm)
}

// refresh 删除所有段文件并创建一个新段。
func (w *diskWAL) refresh() error {
	if err := w.removeAll(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := w.createSegmentFile(w.dir)
	if err != nil {
		return err
	}
	w.fd = f
	w.w = bufio.NewWriterSize(f, w.bufferedSize)
	return nil
}

// createSegmentFile 使用编号索引的名称创建一个新文件。
func (w *diskWAL) createSegmentFile(dir string) (*os.File, error) {
	name := strconv.Itoa(int(atomic.LoadUint32(&w.index)))
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment file: %w", err)
	}
	atomic.AddUint32(&w.index, 1)
	return f, nil
}

type walRecord struct {
	op  walOperation
	row Row
}

type diskWALReader struct {
	dir          string
	files        []os.DirEntry
	rowsToInsert []Row
}

func newDiskWALReader(dir string) (*diskWALReader, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read the WAL dir: %w", err)
	}

	return &diskWALReader{
		dir:          dir,
		files:        files,
		rowsToInsert: make([]Row, 0),
	}, nil
}

// readAll 读取所有段文件并缓存每个操作的结果。
func (f *diskWALReader) readAll() error {
	for _, file := range f.files {
		if file.IsDir() {
			return fmt.Errorf("unexpected directory found under the WAL directory: %s", file.Name())
		}
		fd, err := os.Open(filepath.Join(f.dir, file.Name()))
		if err != nil {
			return fmt.Errorf("failed to open WAL segment file: %w", err)
		}
		segment := &segment{
			file: fd,
			r:    bufio.NewReader(fd),
		}
		for segment.next() {
			rec := segment.record()
			switch rec.op {
			case operationInsert:
				f.rowsToInsert = append(f.rowsToInsert, rec.row)
			}
		}
		if err := segment.close(); err != nil {
			return err
		}

		err = segment.error()
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			// 一行无效并不罕见，因为它可能在写入 WAL 的过程中终止。
			return nil
		}
		if err != nil {
			return fmt.Errorf("encounter an error while reading WAL segment file %q: %w", file.Name(), segment.error())
		}
	}
	return nil
}

// segment 表示一个段文件。
type segment struct {
	file *os.File
	r    *bufio.Reader
	// FIXME: 使用接口来支持其他操作类型
	current walRecord
	err     error
}

func (f *segment) next() bool {
	op, err := f.r.ReadByte()
	if errors.Is(err, io.EOF) {
		return false
	}
	if err != nil {
		f.err = err
		return false
	}
	switch walOperation(op) {
	case operationInsert:
		// 读取指标名称的长度。
		metricLen, err := binary.ReadUvarint(f.r)
		if err != nil {
			f.err = fmt.Errorf("failed to read the length of metric name: %w", err)
			return false
		}
		// 读取指标名称。
		metric := make([]byte, int(metricLen))
		if _, err := io.ReadFull(f.r, metric); err != nil {
			f.err = fmt.Errorf("failed to read the metric name: %w", err)
			return false
		}
		// 读取时间戳。
		ts, err := binary.ReadVarint(f.r)
		if err != nil {
			f.err = fmt.Errorf("failed to read timestamp: %w", err)
			return false
		}
		// 读取值。
		val, err := binary.ReadUvarint(f.r)
		if err != nil {
			f.err = fmt.Errorf("failed to read value: %w", err)
			return false
		}
		f.current = walRecord{
			op: walOperation(op),
			row: Row{
				Metric: string(metric),
				DataPoint: DataPoint{
					Timestamp: ts,
					Value:     math.Float64frombits(val),
				},
			},
		}
	default:
		f.err = fmt.Errorf("unknown operation %v found", op)
		return false
	}

	return true
}

// error 如果在读取过程中遇到错误，则返回错误。
func (f *segment) error() error {
	return f.err
}

func (f *segment) record() *walRecord {
	return &f.current
}

func (f *segment) close() error {
	return f.file.Close()
}
