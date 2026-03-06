package tstorage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/nakabonne/tstorage/internal/syscall"
)

const (
	dataFileName = "data"
	metaFileName = "meta.json"
)

var (
	errInvalidPartition = errors.New("invalid partition")
)

// diskPartition 实现了一个使用本地磁盘作为存储的分区。
	// 它主要有两个文件：数据文件和元数据文件。
	// 数据文件是内存映射的且只读的；完全不需要加锁。
type diskPartition struct {
	dirPath string
	meta    meta
	// 数据文件的文件描述符
	f *os.File
	// 由 f 支持的内存映射文件
	mappedFile []byte
	// 存储数据的持续时间
	retention time.Duration
}

// meta 是元数据文件的映射器，为每个分区放置一个。
	// 注意，CreatedAt 肯定是由 tstorage 加时间戳的，但 Min/Max 时间戳很可能由其他进程完成。
type meta struct {
	MinTimestamp  int64                 `json:"minTimestamp"`
	MaxTimestamp  int64                 `json:"maxTimestamp"`
	NumDataPoints int                   `json:"numDataPoints"`
	Metrics       map[string]diskMetric `json:"metrics"`
	CreatedAt     time.Time             `json:"createdAt"`
}

// diskMetric 保存用于从内存映射文件访问实际数据的元数据。
type diskMetric struct {
	Name          string `json:"name"`
	Offset        int64  `json:"offset"`
	MinTimestamp  int64  `json:"minTimestamp"`
	MaxTimestamp  int64  `json:"maxTimestamp"`
	NumDataPoints int64  `json:"numDataPoints"`
}

// openDiskPartition 首先使用内存映射将数据文件映射到内存中。
func openDiskPartition(dirPath string, retention time.Duration) (partition, error) {
	if dirPath == "" {
		return nil, fmt.Errorf("dir path is required")
	}
	metaFilePath := filepath.Join(dirPath, metaFileName)
	_, err := os.Stat(metaFilePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, errInvalidPartition
	}

	// 将数据映射到内存
	dataPath := filepath.Join(dirPath, dataFileName)
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read data file: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch file info: %w", err)
	}
	if info.Size() == 0 {
		return nil, ErrNoDataPoints
	}
	mapped, err := syscall.Mmap(int(f.Fd()), int(info.Size()))
	if err != nil {
		return nil, fmt.Errorf("failed to perform mmap: %w", err)
	}

	// 将元数据读取到堆中
	m := meta{}
	mf, err := os.Open(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	defer mf.Close()
	decoder := json.NewDecoder(mf)
	if err := decoder.Decode(&m); err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}
	return &diskPartition{
		dirPath:    dirPath,
		meta:       m,
		f:          f,
		mappedFile: mapped,
		retention:  retention,
	}, nil
}

func (d *diskPartition) insertRows(_ []Row) ([]Row, error) {
	return nil, fmt.Errorf("can't insert rows into disk partition")
}

func (d *diskPartition) selectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {
	if d.expired() {
		return nil, fmt.Errorf("this partition is expired: %w", ErrNoDataPoints)
	}
	name := marshalMetricName(metric, labels)
	mt, ok := d.meta.Metrics[name]
	if !ok {
		return nil, ErrNoDataPoints
	}
	r := bytes.NewReader(d.mappedFile)
	if _, err := r.Seek(mt.Offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}
	decoder, err := newSeriesDecoder(r)
	if err != nil {
		return nil, fmt.Errorf("failed to generate decoder for metric %q in %q: %w", name, d.dirPath, err)
	}

	// TODO: 刷新时将固定长度的块分开，并对其进行索引。
	points := make([]*DataPoint, 0, mt.NumDataPoints)
	for i := 0; i < int(mt.NumDataPoints); i++ {
		point := &DataPoint{}
		if err := decoder.decodePoint(point); err != nil {
			return nil, fmt.Errorf("failed to decode point of metric %q in %q: %w", name, d.dirPath, err)
		}
		if point.Timestamp < start {
			continue
		}
		if point.Timestamp >= end {
			break
		}
		points = append(points, point)
	}
	return points, nil
}

func (d *diskPartition) minTimestamp() int64 {
	return d.meta.MinTimestamp
}

func (d *diskPartition) maxTimestamp() int64 {
	return d.meta.MaxTimestamp
}

func (d *diskPartition) size() int {
	return d.meta.NumDataPoints
}

// 磁盘分区是不可变的。
func (d *diskPartition) active() bool {
	return false
}

func (d *diskPartition) clean() error {
	if err := os.RemoveAll(d.dirPath); err != nil {
		return fmt.Errorf("failed to remove all files inside the partition (%d~%d): %w", d.minTimestamp(), d.maxTimestamp(), err)
	}

	return nil
}

func (d *diskPartition) expired() bool {
	diff := time.Since(d.meta.CreatedAt)
	if diff > d.retention {
		return true
	}
	return false
}
