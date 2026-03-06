package tstorage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/nakabonne/tstorage/internal/cgroup"
	"github.com/nakabonne/tstorage/internal/timerpool"
)

var (
	ErrNoDataPoints = errors.New("no data points found")

	// 将数据摄取的并发限制为 GOMAXPROCS，因为此操作是 CPU 密集型的，
	// 所以在数据摄取路径上运行超过 GOMAXPROCS 的并发 goroutine 没有意义。
	defaultWorkersLimit = cgroup.AvailableCPUs()

	partitionDirRegex = regexp.MustCompile(`^p-.+`)
)

// TimestampPrecision 表示时间戳的精度。参见 WithTimestampPrecision
type TimestampPrecision string

const (
	Nanoseconds  TimestampPrecision = "ns"
	Microseconds TimestampPrecision = "us"
	Milliseconds TimestampPrecision = "ms"
	Seconds      TimestampPrecision = "s"

	defaultPartitionDuration  = 1 * time.Hour
	defaultRetention          = 336 * time.Hour
	defaultTimestampPrecision = Nanoseconds
	defaultWriteTimeout       = 30 * time.Second
	defaultWALBufferedSize    = 4096

	writablePartitionsNum = 2
	checkExpiredInterval  = time.Hour

	walDirName = "wal"
)

// Storage 提供了向时序存储插入和检索数据的 goroutine 安全功能。
type Storage interface {
	Reader
	// InsertRows 将给定的行摄取到时序存储中。
	// 如果时间戳为空，则使用机器的 UTC 本地时间戳。
	// 时间戳的精度默认为纳秒。可以使用 WithTimestampPrecision 更改。
	InsertRows(rows []Row) error
	// Close 通过将任何未写入的数据刷新到底层磁盘分区来优雅地关闭。
	Close() error
}

// Reader 提供对时序数据的读取访问。
type Reader interface {
	// Select 返回在给定的 start-end 范围内匹配给定指标和标签的一组数据点。
	// 请注意，start 是包含的，end 是排除的，两者都必须是 Unix 时间戳。
	// 如果没有找到数据点，将返回 ErrNoDataPoints。
	Select(metric string, labels []Label, start, end int64) (points []*DataPoint, err error)
}

// Row 包含一个数据点以及用于标识一种指标的属性。
type Row struct {
	// 指标的唯一名称。
	// 必须设置此字段。
	Metric string
	// 用于进一步详细标识的可选键值属性。
	Labels []Label
	// 必须设置此字段。
	DataPoint
}

// DataPoint 表示一个数据点，是时序数据的最小单位。
type DataPoint struct {
	// 实际值。必须设置此字段。
	Value float64
	// Unix 时间戳。
	Timestamp int64
}

// Option 是 NewStorage 的可选设置。
type Option func(*storage)

// WithDataPath 指定存储时序数据的目录路径。
// 使用此选项使时序数据在磁盘上持久化。
//
// 默认为空字符串，意味着不会持久化任何数据。
func WithDataPath(dataPath string) Option {
	return func(s *storage) {
		s.dataPath = dataPath
	}
}

// WithPartitionDuration 指定分区的时间戳范围。
// 一旦超过给定的时间范围，就会插入新的分区。
//
// 分区是带有时间戳范围的时序数据块。
// 它作为一个完全独立的数据库，包含其时间范围内的所有数据点。
//
// 默认为 1 小时
func WithPartitionDuration(duration time.Duration) Option {
	return func(s *storage) {
		s.partitionDuration = duration
	}
}

// WithRetention 指定何时删除旧数据。
// 数据点将在磁盘分区创建后的指定时间后自动从磁盘上删除。
// 默认为 14 天。
func WithRetention(retention time.Duration) Option {
	return func(s *storage) {
		s.retention = retention
	}
}

// WithTimestampPrecision 指定所有操作使用的时间戳精度。
//
// 默认为纳秒
func WithTimestampPrecision(precision TimestampPrecision) Option {
	return func(s *storage) {
		s.timestampPrecision = precision
	}
}

// WithWriteTimeout 指定 worker 忙碌时等待的超时时间。
//
// 存储限制并发 goroutine 的数量，以防止内存溢出错误和 CPU 争用，
// 即使有太多 goroutine 尝试写入也是如此。
//
// 默认为 30 秒。
func WithWriteTimeout(timeout time.Duration) Option {
	return func(s *storage) {
		s.writeTimeout = timeout
	}
}

// WithLogger 指定用于输出详细日志的记录器。
//
// 默认为不执行任何操作的记录器实现。
func WithLogger(logger Logger) Option {
	return func(s *storage) {
		s.logger = logger
	}
}

// WithWAL 指定在刷新 WAL 文件之前的缓冲区字节大小。
// 缓冲区越大，文件写入频率越低，写入性能越高，但会降低持久性。
// 给定 0 表示每当数据点到来时都写入文件。
// 给定 -1 表示禁用 WAL。
//
// 默认为 4096。
func WithWALBufferedSize(size int) Option {
	return func(s *storage) {
		s.walBufferedSize = size
	}
}

// NewStorage 返回一个新的存储，默认在进程内存中存储时序数据。
//
// 提供WithDataPath 选项以作为磁盘存储运行。指定一个已存在数据的目录，
// 然后它将被读取为初始数据。
func NewStorage(opts ...Option) (Storage, error) {
	s := &storage{
		partitionList:      newPartitionList(),
		workersLimitCh:     make(chan struct{}, defaultWorkersLimit),
		partitionDuration:  defaultPartitionDuration,
		retention:          defaultRetention,
		timestampPrecision: defaultTimestampPrecision,
		writeTimeout:       defaultWriteTimeout,
		walBufferedSize:    defaultWALBufferedSize,
		wal:                &nopWAL{},
		logger:             &nopLogger{},
		doneCh:             make(chan struct{}, 0),
	}
	for _, opt := range opts {
		opt(s)
	}

	if s.inMemoryMode() {
		s.newPartition(nil, false)
		return s, nil
	}

	if err := os.MkdirAll(s.dataPath, fs.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to make data directory %s: %w", s.dataPath, err)
	}

	walDir := filepath.Join(s.dataPath, walDirName)
	if s.walBufferedSize >= 0 {
		wal, err := newDiskWAL(walDir, s.walBufferedSize)
		if err != nil {
			return nil, err
		}
		s.wal = wal
	}

	// Read existent partitions from the disk.
	dirs, err := os.ReadDir(s.dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data directory: %w", err)
	}
	if len(dirs) == 0 {
		s.newPartition(nil, false)
		return s, nil
	}
	isPartitionDir := func(f fs.DirEntry) bool {
		return f.IsDir() && partitionDirRegex.MatchString(f.Name())
	}
	partitions := make([]partition, 0, len(dirs))
	for _, e := range dirs {
		if !isPartitionDir(e) {
			continue
		}
		path := filepath.Join(s.dataPath, e.Name())
		part, err := openDiskPartition(path, s.retention)
		if errors.Is(err, ErrNoDataPoints) {
			continue
		}
		if errors.Is(err, errInvalidPartition) {
			// It should be recovered by WAL
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to open disk partition for %s: %w", path, err)
		}
		partitions = append(partitions, part)
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].minTimestamp() < partitions[j].minTimestamp()
	})
	for _, p := range partitions {
		s.newPartition(p, false)
	}
	// Start WAL recovery if there is.
	if err := s.recoverWAL(walDir); err != nil {
		return nil, fmt.Errorf("failed to recover WAL: %w", err)
	}
	s.newPartition(nil, false)

	// periodically check and permanently remove expired partitions.
	go func() {
		ticker := time.NewTicker(checkExpiredInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.doneCh:
				return
			case <-ticker.C:
				err := s.removeExpiredPartitions()
				if err != nil {
					s.logger.Printf("%v\n", err)
				}
			}
		}
	}()
	return s, nil
}

type storage struct {
	partitionList partitionList

	walBufferedSize    int
	wal                wal
	partitionDuration  time.Duration
	retention          time.Duration
	timestampPrecision TimestampPrecision
	dataPath           string
	writeTimeout       time.Duration

	logger         Logger
	workersLimitCh chan struct{}
	// wg 必须递增以保证所有写入都能优雅地完成。
	wg sync.WaitGroup

	doneCh chan struct{}
}

func (s *storage) InsertRows(rows []Row) error {
	s.wg.Add(1)
	defer s.wg.Done()

	insert := func() error {
		defer func() { <-s.workersLimitCh }()
		if err := s.ensureActiveHead(); err != nil {
			return err
		}
		iterator := s.partitionList.newIterator()
		n := s.partitionList.size()
		rowsToInsert := rows
		// 从头部分区开始，尝试插入行，并循环将过时的行插入到较旧的分区中。
		// 任何超过 `writablePartitionsNum` 个分区过期的行都会被丢弃。
		for i := 0; i < n && i < writablePartitionsNum; i++ {
			if len(rowsToInsert) == 0 {
				break
			}
			if !iterator.next() {
				break
			}
			outdatedRows, err := iterator.value().insertRows(rowsToInsert)
			if err != nil {
				return fmt.Errorf("failed to insert rows: %w", err)
			}
			rowsToInsert = outdatedRows
		}
		return nil
	}

	// 限制并发 goroutine 的数量，以防止内存溢出错误和 CPU 争用，
	// 即使有太多 goroutine 尝试写入也是如此。
	select {
	case s.workersLimitCh <- struct{}{}:
		return insert()
	default:
	}

	// 看起来所有 worker 都很忙；最多等待 writeTimeout

	t := timerpool.Get(s.writeTimeout)
	select {
	case s.workersLimitCh <- struct{}{}:
		timerpool.Put(t)
		return insert()
	case <-t.C:
		timerpool.Put(t)
		return fmt.Errorf("failed to write a data point in %s, since it is overloaded with %d concurrent writers",
			s.writeTimeout, defaultWorkersLimit)
	}
}

// ensureActiveHead 确保 partitionList 的头部是一个活动的分区。
// 如果没有，则创建一个新的。
func (s *storage) ensureActiveHead() error {
	head := s.partitionList.getHead()
	if head != nil && head.active() {
		return nil
	}

	// 所有分区似乎都是不活动的，因此向列表中添加一个新分区。
	if err := s.newPartition(nil, true); err != nil {
		return err
	}
	go func() {
		if err := s.flushPartitions(); err != nil {
			s.logger.Printf("failed to flush in-memory partitions: %v", err)
		}
	}()
	return nil
}

func (s *storage) Select(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {
	if metric == "" {
		return nil, fmt.Errorf("metric must be set")
	}
	if start >= end {
		return nil, fmt.Errorf("the given start is greater than end")
	}
	points := make([]*DataPoint, 0)

	// 从最新的分区开始遍历所有分区。
	iterator := s.partitionList.newIterator()
	for iterator.next() {
		part := iterator.value()
		if part == nil {
			return nil, fmt.Errorf("unexpected empty partition found")
		}
		if part.minTimestamp() == 0 {
			// 跳过没有数据点的分区。
			continue
		}
		if part.maxTimestamp() < start {
			// 不需要继续了
			break
		}
		if part.minTimestamp() > end {
			continue
		}
		ps, err := part.selectDataPoints(metric, labels, start, end)
		if errors.Is(err, ErrNoDataPoints) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to select data points: %w", err)
		}
		// 为了保持升序。
		points = append(ps, points...)
	}
	if len(points) == 0 {
		return nil, ErrNoDataPoints
	}
	return points, nil
}

func (s *storage) Close() error {
	s.wg.Wait()
	close(s.doneCh)
	if err := s.wal.flush(); err != nil {
		return fmt.Errorf("failed to flush buffered WAL: %w", err)
	}

	// TODO: 防止新的 goroutine 调用 InsertRows()，以实现优雅关闭。

	// 通过插入相同数量的分区，使所有可写分区变为只读。
	for i := 0; i < writablePartitionsNum; i++ {
		if err := s.newPartition(nil, true); err != nil {
			return err
		}
	}
	if err := s.flushPartitions(); err != nil {
		return fmt.Errorf("failed to close storage: %w", err)
	}
	if err := s.removeExpiredPartitions(); err != nil {
		return fmt.Errorf("failed to remove expired partitions: %w", err)
	}
	// 所有分区都已刷新，因此不再需要 WAL。
	if err := s.wal.removeAll(); err != nil {
		return fmt.Errorf("failed to remove WAL: %w", err)
	}
	return nil
}

func (s *storage) newPartition(p partition, punctuateWal bool) error {
	if p == nil {
		p = newMemoryPartition(s.wal, s.partitionDuration, s.timestampPrecision)
	}
	s.partitionList.insert(p)
	if punctuateWal {
		return s.wal.punctuate()
	}
	return nil
}

// flushPartitions 持久化所有准备好持久化的内存分区。
	// 对于内存模式，只需从分区列表中删除它。
func (s *storage) flushPartitions() error {
	// 保留前两个分区不变，即使它们是不活动的，以接受乱序数据点。
	i := 0
	iterator := s.partitionList.newIterator()
	for iterator.next() {
		if i < writablePartitionsNum {
			i++
			continue
		}
		part := iterator.value()
		if part == nil {
			return fmt.Errorf("unexpected empty partition found")
		}
		memPart, ok := part.(*memoryPartition)
		if !ok {
			continue
		}

		if s.inMemoryMode() {
			if err := s.partitionList.remove(part); err != nil {
				return fmt.Errorf("failed to remove partition: %w", err)
			}
			continue
		}

		// 开始将内存分区交换为磁盘分区。
		// 磁盘分区将放置在内存分区所在的位置。

		dir := filepath.Join(s.dataPath, fmt.Sprintf("p-%d-%d", memPart.minTimestamp(), memPart.maxTimestamp()))
		if err := s.flush(dir, memPart); err != nil {
			return fmt.Errorf("failed to compact memory partition into %s: %w", dir, err)
		}
		newPart, err := openDiskPartition(dir, s.retention)
		if errors.Is(err, ErrNoDataPoints) {
			if err := s.partitionList.remove(part); err != nil {
				return fmt.Errorf("failed to remove partition: %w", err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to generate disk partition for %s: %w", dir, err)
		}
		if err := s.partitionList.swap(part, newPart); err != nil {
			return fmt.Errorf("failed to swap partitions: %w", err)
		}

		if err := s.wal.removeOldest(); err != nil {
			return fmt.Errorf("failed to remove oldest WAL segment: %w", err)
		}
	}
	return nil
}

// flush 压缩给定分区中的数据点，并将它们刷新到给定目录。
func (s *storage) flush(dirPath string, m *memoryPartition) error {
	if dirPath == "" {
		return fmt.Errorf("dir path is required")
	}

	if err := os.MkdirAll(dirPath, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to make directory %q: %w", dirPath, err)
	}

	f, err := os.Create(filepath.Join(dirPath, dataFileName))
	if err != nil {
		return fmt.Errorf("failed to create file %q: %w", dirPath, err)
	}
	defer f.Close()
	encoder := newSeriesEncoder(f)

	metrics := map[string]diskMetric{}
	m.metrics.Range(func(key, value interface{}) bool {
		mt, ok := value.(*memoryMetric)
		if !ok {
			s.logger.Printf("unknown value found\n")
			return false
		}
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			s.logger.Printf("failed to set file offset of metric %q: %v\n", mt.name, err)
			return false
		}

		if err := mt.encodeAllPoints(encoder); err != nil {
			s.logger.Printf("failed to encode a data point that metric is %q: %v\n", mt.name, err)
			return false
		}

		if err := encoder.flush(); err != nil {
			s.logger.Printf("failed to flush data points that metric is %q: %v\n", mt.name, err)
			return false
		}

		totalNumPoints := mt.size + int64(len(mt.outOfOrderPoints))
		metrics[mt.name] = diskMetric{
			Name:          mt.name,
			Offset:        offset,
			MinTimestamp:  mt.minTimestamp,
			MaxTimestamp:  mt.maxTimestamp,
			NumDataPoints: totalNumPoints,
		}
		return true
	})

	b, err := json.Marshal(&meta{
		MinTimestamp:  m.minTimestamp(),
		MaxTimestamp:  m.maxTimestamp(),
		NumDataPoints: m.size(),
		Metrics:       metrics,
		CreatedAt:     time.Now(),
	})

	// 应该最后写入 meta 文件，因为有效的 meta 文件存在证明磁盘分区是有效的。
	metaPath := filepath.Join(dirPath, metaFileName)
	if err := os.WriteFile(metaPath, b, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to write metadata to %s: %w", metaPath, err)
	}
	return nil
}

func (s *storage) removeExpiredPartitions() error {
	expiredList := make([]partition, 0)
	iterator := s.partitionList.newIterator()
	for iterator.next() {
		part := iterator.value()
		if part == nil {
			return fmt.Errorf("unexpected nil partition found")
		}
		if part.expired() {
			expiredList = append(expiredList, part)
		}
	}

	for i := range expiredList {
		if err := s.partitionList.remove(expiredList[i]); err != nil {
			return fmt.Errorf("failed to remove expired partition")
		}
	}
	return nil
}

// recoverWAL 插入给定 WAL 中的所有记录，然后删除所有 WAL 段文件。
func (s *storage) recoverWAL(walDir string) error {
	reader, err := newDiskWALReader(walDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	if err := reader.readAll(); err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	if len(reader.rowsToInsert) == 0 {
		return nil
	}
	if err := s.InsertRows(reader.rowsToInsert); err != nil {
		return fmt.Errorf("failed to insert rows recovered from WAL: %w", err)
	}
	return s.wal.refresh()
}

func (s *storage) inMemoryMode() bool {
	return s.dataPath == ""
}
