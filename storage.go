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

// storage 是 Storage 接口的具体实现，负责管理时序数据的存储、检索和生命周期。
type storage struct {
	// partitionList 管理所有的内存分区和磁盘分区，按时间顺序组织
	partitionList partitionList

	// walBufferedSize 指定 WAL（预写日志）的缓冲区大小（字节）
	// 0 表示每次都写入文件，-1 表示禁用 WAL，默认为 4096
	walBufferedSize int

	// wal 是预写日志接口，在数据写入内存分区前先记录日志，防止数据丢失
	wal wal

	// partitionDuration 指定每个分区的时间跨度，超过这个时间会创建新分区
	// 默认为 1 小时
	partitionDuration time.Duration

	// retention 指定数据保留时间，超过此时间的磁盘分区会被自动删除
	// 默认为 14 天（336 小时）
	retention time.Duration

	// timestampPrecision 指定时间戳的精度，支持纳秒、微秒、毫秒、秒
	// 默认为纳秒
	timestampPrecision TimestampPrecision

	// dataPath 指定磁盘存储路径，为空时表示使用内存模式
	dataPath string

	// writeTimeout 指定写入超时时间，当所有 worker 忙碌时的等待时间
	// 默认为 30 秒
	writeTimeout time.Duration

	// logger 是日志记录器接口，用于输出运行时日志
	logger Logger

	// workersLimitCh 是并发限制通道，用于控制同时写入的 goroutine 数量
	// 通道容量为 GOMAXPROCS，防止过多的并发写入导致内存溢出和 CPU 争用
	workersLimitCh chan struct{}

	// wg 是等待组，用于在关闭时等待所有写入操作完成
	// 每次写入前调用 Add()，写入完成后调用 Done()
	wg sync.WaitGroup

	// doneCh 是关闭信号通道，用于通知后台 goroutine（如过期分区清理）退出
	doneCh chan struct{}
}

// InsertRows 将给定的行批量插入到时序存储中。
// 此方法是 goroutine 安全的，可以并发调用。
//
// 工作原理：
// 1. 通过 workersLimitCh 限制并发写入的 goroutine 数量，防止资源耗尽
// 2. 从头部分区开始尝试插入数据
// 3. 如果数据点的时间戳早于当前分区的时间范围，则尝试插入到更旧的分区
// 4. 最多支持 2 个可写分区（writablePartitionsNum），用于处理乱序数据
// 5. 超过可写分区范围的乱序数据会被丢弃
//
// 参数：
//
//	rows - 要插入的数据行数组，每行包含指标名称、标签和数据点
//
// 返回值：
//
//	error - 如果写入失败或超时，返回相应的错误
//
// 注意事项：
//   - 所有数据点在插入内存分区前会先写入 WAL（如果启用）
//   - 如果所有 worker 都在忙碌，会等待 writeTimeout 时间
//   - 超时后会返回错误，数据不会被插入
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

	// 1. 所有分区似乎都是不活动的，因此向列表中添加一个新分区。
	if err := s.newPartition(nil, true); err != nil {
		return err
	}
	// 2. 异步刷新旧的分区到磁盘
	go func() {
		if err := s.flushPartitions(); err != nil {
			s.logger.Printf("failed to flush in-memory partitions: %v", err)
		}
	}()
	return nil
}

// Select 查询指定指标和标签在给定时间范围内的数据点。
// 此方法是 goroutine 安全的，可以并发调用。
//
// 工作原理：
// 1. 从最新的分区开始向前遍历所有分区（包括内存分区和磁盘分区）
// 2. 跳过不包含查询时间范围的分区
// 3. 从每个相关分区中查询匹配的数据点
// 4. 将结果按时间戳升序排序后返回
//
// 参数：
//
//	metric - 要查询的指标名称，不能为空
//	labels - 标签列表，用于进一步筛选指标
//	start - 查询范围的起始时间戳（包含），必须是 Unix 时间戳
//	end - 查询范围的结束时间戳（不包含），必须是 Unix 时间戳
//
// 返回值：
//
//	[]*DataPoint - 匹配的数据点数组，按时间戳升序排序
//	error - 如果查询失败或没有找到数据点，返回相应的错误
//
// 注意事项：
//   - start 必须小于 end，否则返回错误
//   - 如果没有找到匹配的数据点，返回 ErrNoDataPoints 错误
//   - 查询性能与时间范围和分区数量相关
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

// Close 优雅地关闭存储，将所有未写入的数据刷新到磁盘并清理资源。
// 调用此方法后，存储将不再接受新的写入操作。
//
// 工作流程：
// 1. 等待所有正在进行的写入操作完成（通过 WaitGroup）
// 2. 发送关闭信号，停止后台 goroutine（如过期分区清理）
// 3. 刷新 WAL 中的缓冲数据到磁盘
// 4. 将所有可写的内存分区强制转换为只读
// 5. 刷新所有内存分区到磁盘
// 6. 删除过期的分区
// 7. 清理所有 WAL 文件
//
// 返回值：
//
//	error - 如果关闭过程中出现错误，返回相应的错误
//
// 注意事项：
//   - 关闭过程中无法插入新数据
//   - 确保所有数据已正确刷新到磁盘后才会删除 WAL
//   - 关闭后存储对象不可再使用
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

// newPartition 向分区列表中添加一个新分区。
//
// 参数：
//
//	p - 要添加的分区，如果为 nil 则创建新的内存分区
//	punctuateWal - 是否在 WAL 中标记分区边界（插入分隔符）
//
// 返回值：
//
//	error - 如果添加分区失败，返回相应的错误
//
// 注意事项：
//   - 新分区会被插入到分区列表的头部
//   - 如果 punctuateWal 为 true，会在 WAL 中写入边界标记
//   - 边界标记用于 WAL 恢复时确定哪些数据属于哪个分区
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

// flushPartitions 将所有准备好持久化的内存分区刷新到磁盘。
// 对于内存模式，直接从分区列表中删除分区。
//
// 工作流程：
// 1. 保留前两个分区不变（用于接受乱序数据点）
// 2. 遍历剩余的分区，查找可持久化的内存分区
// 3. 对于每个内存分区：
//   - 内存模式：直接从分区列表中删除
//   - 磁盘模式：压缩数据并写入磁盘，然后替换为磁盘分区
//
// 4. 刷新成功后，删除对应的 WAL 段文件
//
// 返回值：
//
//	error - 如果刷新过程中出现错误，返回相应的错误
//
// 注意事项：
//   - 保留的两个可写分区可以处理时间戳较早的乱序数据
//   - 磁盘分区使用 mmap 映射，只读
//   - 刷新过程中会创建新的磁盘分区目录（格式：p-{minTimestamp}-{maxTimestamp}）
//   - 每个磁盘分区包含 data 文件和 meta.json 文件
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

		// 在内存模式下，直接从分区列表中删除分区，数据不持久化
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

// flush 将内存分区中的数据点压缩并写入到指定目录，创建磁盘分区。
//
// 工作流程：
// 1. 创建目标目录（如果不存在）
// 2. 创建 data 文件，用于存储压缩后的数据点
// 3. 遍历内存分区中的所有指标：
//   - 记录当前文件偏移量
//   - 编码该指标的所有数据点并写入文件
//   - 刷新缓冲区确保数据写入磁盘
//   - 记录指标的元数据（名称、偏移量、时间范围、数据点数量）
//
// 4. 创建 meta.json 文件，存储分区的元数据
// 5. 最后写入 meta 文件，确保磁盘分区完整性
//
// 参数：
//
//	dirPath - 目标目录路径，不能为空
//	m - 要刷新的内存分区
//
// 返回值：
//
//	error - 如果刷新过程中出现错误，返回相应的错误
//
// 注意事项：
//   - 每个指标的数据点单独编码和压缩
//   - meta.json 文件必须最后写入，作为分区有效的证明
//   - 磁盘分区是只读的，使用 mmap 映射访问
func (s *storage) flush(dirPath string, m *memoryPartition) error {
	// 验证目标目录路径是否有效
	if dirPath == "" {
		return fmt.Errorf("dir path is required")
	}

	// 创建目标目录，用于存储磁盘分区的数据文件和元数据
	if err := os.MkdirAll(dirPath, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to make directory %q: %w", dirPath, err)
	}

	// 创建 data 文件，用于存储压缩后的数据点
	// 数据点会按指标分别编码和压缩后写入此文件
	f, err := os.Create(filepath.Join(dirPath, dataFileName))
	if err != nil {
		return fmt.Errorf("failed to create file %q: %w", dirPath, err)
	}
	defer f.Close()
	// 创建序列编码器，用于将数据点编码写入文件
	encoder := newSeriesEncoder(f)

	// 存储每个指标的元数据，包括名称、文件偏移量、时间范围等
	metrics := map[string]diskMetric{}

	// 遍历内存分区中的所有指标，将每个指标的数据点写入磁盘
	m.metrics.Range(func(key, value interface{}) bool {
		// 类型断言，确保值是 memoryMetric 类型
		mt, ok := value.(*memoryMetric)
		if !ok {
			s.logger.Printf("unknown value found\n")
			return false
		}

		// 获取当前文件偏移量，记录该指标数据在文件中的起始位置
		// 这个偏移量将被记录在元数据中，用于后续快速定位读取
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			s.logger.Printf("failed to set file offset of metric %q: %v\n", mt.name, err)
			return false
		}

		// 将该指标的所有数据点（包括正常点和乱序点）编码并写入文件
		// 数据点会被压缩以节省存储空间
		if err := mt.encodeAllPoints(encoder); err != nil {
			s.logger.Printf("failed to encode a data point that metric is %q: %v\n", mt.name, err)
			return false
		}

		// 刷新编码器缓冲区，确保数据写入磁盘
		// 避免数据缓存在内存中导致丢失
		if err := encoder.flush(); err != nil {
			s.logger.Printf("failed to flush data points that metric is %q: %v\n", mt.name, err)
			return false
		}

		// 计算该指标的总数据点数量（正常点 + 乱序点）
		totalNumPoints := mt.size + int64(len(mt.outOfOrderPoints))

		// 记录该指标的元数据，用于后续快速查询和数据定位
		metrics[mt.name] = diskMetric{
			Name:          mt.name,
			Offset:        offset,      // 在 data 文件中的起始位置
			MinTimestamp:  mt.minTimestamp,
			MaxTimestamp:  mt.maxTimestamp,
			NumDataPoints: totalNumPoints,
		}
		return true
	})

	// 创建分区元数据，包含分区级别的信息和所有指标的索引
	b, err := json.Marshal(&meta{
		MinTimestamp:  m.minTimestamp(),
		MaxTimestamp:  m.maxTimestamp(),
		NumDataPoints: m.size(),
		Metrics:       metrics,
		CreatedAt:     time.Now(),
	})

	// 应该最后写入 meta 文件，因为有效的 meta 文件存在证明磁盘分区是有效的
	// 如果在写入 meta 文件前程序崩溃，分区将被视为无效，不会被加载
	metaPath := filepath.Join(dirPath, metaFileName)
	if err := os.WriteFile(metaPath, b, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to write metadata to %s: %w", metaPath, err)
	}
	return nil
}

// removeExpiredPartitions 从分区列表中移除所有过期的分区。
// 过期判断基于配置的 retention 时间。
//
// 工作流程：
// 1. 遍历所有分区
// 2. 检查每个分区是否已过期（通过 partition.expired() 方法）
// 3. 收集所有过期的分区
// 4. 从分区列表中移除这些分区
//
// 返回值：
//
//	error - 如果移除过程中出现错误，返回相应的错误
//
// 注意事项：
//   - 此函数由后台 goroutine 定期调用（默认每小时一次）
//   - 过期的磁盘分区会被删除（包括其文件）
//   - 过期的内存分区会被直接丢弃
//   - 保留时间从分区创建时开始计算
func (s *storage) removeExpiredPartitions() error {
	// 收集所有过期的分区
	// 先收集再删除，避免在遍历过程中修改分区列表导致的问题
	expiredList := make([]partition, 0)

	// 创建分区列表迭代器，从最新到最旧遍历所有分区
	iterator := s.partitionList.newIterator()

	// 遍历所有分区，检查是否过期
	for iterator.next() {
		part := iterator.value()
		if part == nil {
			return fmt.Errorf("unexpected nil partition found")
		}

		// 检查分区是否已过期
		// 过期判断基于配置的 retention 时间
		// 内存分区永远不会过期（expired() 返回 false）
		// 磁盘分区会根据创建时间检查是否超过 retention
		if part.expired() {
			// 将过期的分区添加到待删除列表
			expiredList = append(expiredList, part)
		}
	}

	// 遍历所有过期的分区，从分区列表中移除
	for i := range expiredList {
		if err := s.partitionList.remove(expiredList[i]); err != nil {
			return fmt.Errorf("failed to remove expired partition")
		}
		// 注意：磁盘分区在移除时，其对应的文件也会被删除
		// 这是通过 diskPartition 的 clean() 方法实现的
	}
	return nil
}

// recoverWAL 从 WAL（预写日志）中恢复所有未持久化的数据，然后清理 WAL 文件。
// 此函数在存储启动时调用，用于恢复因崩溃等原因未写入磁盘的数据。
//
// 工作流程：
// 1. 创建 WAL 读取器
// 2. 如果 WAL 目录不存在，直接返回（无需恢复）
// 3. 读取所有 WAL 段文件中的记录
// 4. 将恢复的数据行插入到存储中
// 5. 刷新并清理所有 WAL 段文件
//
// 参数：
//
//	walDir - WAL 目录路径
//
// 返回值：
//
//	error - 如果恢复过程中出现错误，返回相应的错误
//
// 注意事项：
//   - WAL 恢复是在磁盘分区加载之后进行的
//   - 恢复的数据可能跨越多个分区
//   - 恢复完成后，所有 WAL 文件都会被删除
//   - 如果 WAL 文件损坏，恢复可能会失败
func (s *storage) recoverWAL(walDir string) error {
	// 创建 WAL 读取器，用于读取 WAL 文件中的操作记录
	reader, err := newDiskWALReader(walDir)

	// 如果 WAL 目录不存在，说明没有需要恢复的数据
	// 这可能是首次启动或之前正常关闭（WAL 已清理）
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	// 如果出现其他错误（如权限问题、文件损坏等），直接返回错误
	if err != nil {
		return err
	}

	// 读取所有 WAL 段文件中的记录
	// WAL 可能包含多个段文件，按时间顺序排列
	// readAll() 会解析所有记录并还原为数据行
	if err := reader.readAll(); err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	// 如果没有需要恢复的数据，直接返回
	if len(reader.rowsToInsert) == 0 {
		return nil
	}

	// 将恢复的数据行插入到存储中
	// 这些数据会根据时间戳分配到相应的分区（内存或磁盘）
	// 注意：此时磁盘分区已经加载完成，所以数据会被正确分配
	if err := s.InsertRows(reader.rowsToInsert); err != nil {
		return fmt.Errorf("failed to insert rows recovered from WAL: %w", err)
	}

	// 刷新 WAL 并清理所有 WAL 段文件
	// 因为数据已经恢复并持久化，不再需要 WAL 文件
	// refresh() 会删除所有 WAL 段文件，为新的写入操作做准备
	return s.wal.refresh()
}

// inMemoryMode 判断存储是否运行在内存模式。
//
// 返回值：
//
//	bool - true 表示内存模式，false 表示磁盘模式
//
// 注意事项：
//   - 内存模式下数据不持久化到磁盘
//   - 内存模式下不使用 WAL
//   - 内存模式下分区满了之后会被丢弃而不是刷新到磁盘
//   - 磁盘模式下会持久化数据到 dataPath 指定的目录
func (s *storage) inMemoryMode() bool {
	return s.dataPath == ""
}
