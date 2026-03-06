package tstorage

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// memoryPartition 实现了一个在堆上存储数据点的分区。
	// 它提供了 goroutine 安全的功能。
type memoryPartition struct {
	// 数据点的数量
	numPoints int64
	// minT 是不可变的。
	minT int64
	maxT int64

	// 从指标名称到 memoryMetric 的哈希映射。
	metrics sync.Map

	// 预写日志。
	wal wal
	// 分区持久化后的时间戳范围
	partitionDuration  int64
	timestampPrecision TimestampPrecision
	once               sync.Once
}

func newMemoryPartition(wal wal, partitionDuration time.Duration, precision TimestampPrecision) partition {
	if wal == nil {
		wal = &nopWAL{}
	}
	var d int64
	switch precision {
	case Nanoseconds:
		d = partitionDuration.Nanoseconds()
	case Microseconds:
		d = partitionDuration.Microseconds()
	case Milliseconds:
		d = partitionDuration.Milliseconds()
	case Seconds:
		d = int64(partitionDuration.Seconds())
	default:
		d = partitionDuration.Nanoseconds()
	}
	return &memoryPartition{
		partitionDuration:  d,
		wal:                wal,
		timestampPrecision: precision,
	}
}

// insertRows 将给定的行插入到分区中。
func (m *memoryPartition) insertRows(rows []Row) ([]Row, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows given")
	}
	// FIXME: 仅发出日志就足够了
	err := m.wal.append(operationInsert, rows)
	if err != nil {
		return nil, fmt.Errorf("failed to write to WAL: %w", err)
	}

	// 仅在第一次时设置最小时间戳。
	m.once.Do(func() {
		min := rows[0].Timestamp
		for i := range rows {
			row := rows[i]
			if row.Timestamp < min {
				min = row.Timestamp
			}
		}
		atomic.StoreInt64(&m.minT, min)
	})

	outdatedRows := make([]Row, 0)
	maxTimestamp := rows[0].Timestamp
	var rowsNum int64
	for i := range rows {
		row := rows[i]
		if row.Timestamp < m.minTimestamp() {
			outdatedRows = append(outdatedRows, row)
			continue
		}
		if row.Timestamp == 0 {
			row.Timestamp = toUnix(time.Now(), m.timestampPrecision)
		}
		if row.Timestamp > maxTimestamp {
			maxTimestamp = row.Timestamp
		}
		name := marshalMetricName(row.Metric, row.Labels)
		mt := m.getMetric(name)
		mt.insertPoint(&row.DataPoint)
		rowsNum++
	}
	atomic.AddInt64(&m.numPoints, rowsNum)

	// 更新最大时间戳。
	if atomic.LoadInt64(&m.maxT) < maxTimestamp {
		atomic.SwapInt64(&m.maxT, maxTimestamp)
	}

	return outdatedRows, nil
}

func toUnix(t time.Time, precision TimestampPrecision) int64 {
	switch precision {
	case Nanoseconds:
		return t.UnixNano()
	case Microseconds:
		return t.UnixNano() / 1e3
	case Milliseconds:
		return t.UnixNano() / 1e6
	case Seconds:
		return t.Unix()
	default:
		return t.UnixNano()
	}
}

func (m *memoryPartition) selectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {
	name := marshalMetricName(metric, labels)
	mt := m.getMetric(name)
	return mt.selectPoints(start, end), nil
}

// getMetric 返回名称为给定名称的指标列表的引用。
	// 如果不存在，则创建一个新的。
func (m *memoryPartition) getMetric(name string) *memoryMetric {
	value, ok := m.metrics.Load(name)
	if !ok {
		value = &memoryMetric{
			name:             name,
			points:           make([]*DataPoint, 0, 1000),
			outOfOrderPoints: make([]*DataPoint, 0),
		}
		m.metrics.Store(name, value)
	}
	return value.(*memoryMetric)
}

func (m *memoryPartition) minTimestamp() int64 {
	return atomic.LoadInt64(&m.minT)
}

func (m *memoryPartition) maxTimestamp() int64 {
	return atomic.LoadInt64(&m.maxT)
}

func (m *memoryPartition) size() int {
	return int(atomic.LoadInt64(&m.numPoints))
}

func (m *memoryPartition) active() bool {
	return m.maxTimestamp()-m.minTimestamp()+1 < m.partitionDuration
}

func (m *memoryPartition) clean() error {
	// memoryPartition 管理的所有数据都在堆上，会自动被 GC 删除。
	// 所以什么都不做。
	return nil
}

func (m *memoryPartition) expired() bool {
	return false
}

// memoryMetric 具有属于该指标的有序数据点列表
type memoryMetric struct {
	name         string
	size         int64
	minTimestamp int64
	maxTimestamp int64
	// points 必须保持有序
	points           []*DataPoint
	outOfOrderPoints []*DataPoint
	mu               sync.RWMutex
}

func (m *memoryMetric) insertPoint(point *DataPoint) {
	size := atomic.LoadInt64(&m.size)
	// TODO: 考虑停止每次都使用互斥锁。
	//   相反，修复 points 切片的容量，类似于：
	/*
		m.points := make([]*DataPoint, 1000)
		for i := 0; i < 1000; i++ {
			m.points[i] = point
		}
	*/
	m.mu.Lock()
	defer m.mu.Unlock()

	// 第一次插入
	if size == 0 {
		m.points = append(m.points, point)
		atomic.StoreInt64(&m.minTimestamp, point.Timestamp)
		atomic.StoreInt64(&m.maxTimestamp, point.Timestamp)
		atomic.AddInt64(&m.size, 1)
		return
	}
	// 按顺序插入点
	if m.points[size-1].Timestamp < point.Timestamp {
		m.points = append(m.points, point)
		atomic.StoreInt64(&m.maxTimestamp, point.Timestamp)
		atomic.AddInt64(&m.size, 1)
		return
	}

	m.outOfOrderPoints = append(m.outOfOrderPoints, point)
}

// selectPoints 通过使用 [startIdx:endIdx] 重新切片来返回一个新切片。
func (m *memoryMetric) selectPoints(start, end int64) []*DataPoint {
	size := atomic.LoadInt64(&m.size)
	minTimestamp := atomic.LoadInt64(&m.minTimestamp)
	maxTimestamp := atomic.LoadInt64(&m.maxTimestamp)
	var startIdx, endIdx int

	if end <= minTimestamp {
		return []*DataPoint{}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if start <= minTimestamp {
		startIdx = 0
	} else {
		// 使用二分查找，因为 points 是有序的。
		startIdx = sort.Search(int(size), func(i int) bool {
			return m.points[i].Timestamp >= start
		})
	}

	if end >= maxTimestamp {
		endIdx = int(size)
	} else {
		// 使用二分查找，因为 points 是有序的。
		endIdx = sort.Search(int(size), func(i int) bool {
			return m.points[i].Timestamp >= end
		})
	}
	return m.points[startIdx:endIdx]
}

// encodeAllPoints 使用给定的 seriesEncoder 按时间戳顺序编码所有指标数据点，包括 outOfOrderPoints。
func (m *memoryMetric) encodeAllPoints(encoder seriesEncoder) error {
	sort.Slice(m.outOfOrderPoints, func(i, j int) bool {
		return m.outOfOrderPoints[i].Timestamp < m.outOfOrderPoints[j].Timestamp
	})

	var oi, pi int
	for oi < len(m.outOfOrderPoints) && pi < len(m.points) {
		if m.outOfOrderPoints[oi].Timestamp < m.points[pi].Timestamp {
			if err := encoder.encodePoint(m.outOfOrderPoints[oi]); err != nil {
				return err
			}
			oi++
		} else {
			if err := encoder.encodePoint(m.points[pi]); err != nil {
				return err
			}
			pi++
		}
	}
	for oi < len(m.outOfOrderPoints) {
		if err := encoder.encodePoint(m.outOfOrderPoints[oi]); err != nil {
			return err
		}
		oi++
	}
	for pi < len(m.points) {
		if err := encoder.encodePoint(m.points[pi]); err != nil {
			return err
		}
		pi++
	}

	return nil
}
