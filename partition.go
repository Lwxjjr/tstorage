package tstorage

// partition 是一个带有时间戳范围的时序数据块。
// 分区作为一个完全独立的数据库，包含其时间范围内的所有数据点。
//
// 分区的生命周期为：可写 -> 只读。
// *可写*：
//   可以写入数据。分区列表中只能有一个分区是可写的。
// *只读*：
//   不能写入数据。如果分区超出了分区范围，则变为只读。
type partition interface {
	// 写入操作
	//
	// insertRows 是一种 goroutine 安全的方式，用于向自身插入数据点。
	// 如果给定的数据点早于其最小时间戳，则不会被摄取，而是作为第一个返回值返回。
	insertRows(rows []Row) (outdatedRows []Row, err error)
	// clean 删除由此分区管理的所有内容。
	clean() error

	// 读取操作
	//
	// selectDataPoints 返回给定范围内某个指标的数据点。
	selectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error)
	// minTimestamp 返回最小的 Unix 时间戳（毫秒）。
	minTimestamp() int64
	// maxTimestamp 返回最大的 Unix 时间戳（毫秒）。
	maxTimestamp() int64
	// size 返回分区持有的数据点数量。
	size() int
	// active 表示不仅可写，还具有作为头部分区的特性。
	active() bool
	// expired 表示应该被删除。
	expired() bool
}
