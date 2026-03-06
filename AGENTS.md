# TStorage 项目上下文文档

## 项目概述

`tstorage` 是一个轻量级的本地磁盘时序数据存储引擎，提供简洁的 API 和高度优化的数据写入能力。该项目专门为处理大量时序数据而设计，特别优化了数据摄取性能，支持 goroutine 安全的写入和读取操作。

### 核心特性

- **分区存储架构**：采用线性数据模型，按时间分区存储数据点，每个分区作为独立的数据库
- **内存和磁盘双模式**：默认在内存中运行，可通过 `WithDataPath` 选项持久化到磁盘
- **高性能写入**：支持并发写入，内置 worker 池限制并发数，防止内存溢出和 CPU 争用
- **WAL（Write-Ahead Log）**：所有写入操作先记录到 WAL，防止数据丢失
- **标签支持**：支持带有标签的指标，通过指标名称和标签组合唯一标识
- **时间戳精度**：支持纳秒、微秒、毫秒、秒四种精度
- **数据保留策略**：支持自动清理过期分区
- **乱序数据处理**：能够处理网络延迟或时钟同步导致的乱序数据点

### 技术栈

- **语言**：Go 1.20+
- **核心依赖**：
  - `github.com/stretchr/testify` - 测试框架
  - `github.com/davecgh/go-spew` - 调试输出
  - `gopkg.in/yaml.v3` - YAML 解析
- **内部包**：
  - `internal/cgroup` - CPU 资源检测
  - `internal/syscall` - 内存映射（mmap）系统调用
  - `internal/timerpool` - 定时器池
  - `internal/encoding` - 整数编码

## 项目架构

### 分区系统

项目采用分区架构，将时序数据按时间范围划分为多个分区：

```
Read              Write
  │                 │
  │                 V
  │      ┌───────────────────┐ max: 1600010800
  ├─────>   Memory Partition
  │      └───────────────────┘ min: 1600007201
  │
  │      ┌───────────────────┐ max: 1600007200
  ├─────>   Memory Partition
  │      └───────────────────┘ min: 1600003601
  │
  │      ┌───────────────────┐ max: 1600003600
  └─────>   Disk Partition
         └───────────────────┘ min: 1600000000
```

### 分区类型

1. **Memory Partition（内存分区）**
   - 可写入，数据存储在堆中
   - 头部分区始终是内存分区
   - 保留 2 个可写分区以接受乱序数据
   - 使用有序切片存储数据点，提供良好的缓存命中率
   - 所有数据先写入 WAL，再插入内存分区

2. **Disk Partition（磁盘分区）**
   - 只读，数据持久化到磁盘
   - 每个分区包含两个文件：
     - `data`：压缩的数据文件，使用 mmap 内存映射
     - `meta.json`：元数据文件，包含分区信息和指标索引
   - 目录命名格式：`p-{minTimestamp}-{maxTimestamp}`

### 核心组件

- **Storage**：主存储接口，提供 `InsertRows` 和 `Select` 方法
- **Partition**：分区接口，定义分区的读写操作
- **PartitionList**：分区列表，管理所有分区的有序链表
- **WAL**：预写日志，确保持久性
- **Encoder/Decoder**：数据编码/解码器

## 构建和运行

### 测试

```bash
# 运行所有测试（带竞态检测和覆盖率）
make test

# 运行基准测试
make test-bench
```

### 依赖管理

```bash
# 整理依赖
make dep
```

### 文档

```bash
# 启动 godoc 服务器
make godoc
```

### 性能分析

```bash
# 分析内存性能
make pprof-mem

# 分析 CPU 性能
make pprof-cpu
```

### 直接使用 Go 命令

```bash
# 运行测试
go test -race -v -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt ./...

# 运行基准测试
go test -benchtime=4s -benchmem -bench=. .

# 生成文档
go doc -all github.com/nakabonne/tstorage
```

## 开发约定

### 代码风格

- 遵循 Go 标准编码规范
- 使用接口定义抽象，便于测试和扩展
- 关键类型定义清晰的接口：
  - `Storage`：主存储接口
  - `Reader`：只读接口
  - `Partition`：分区接口

### 错误处理

- 使用 `errors.New` 定义明确的错误类型
- 错误信息包含上下文，使用 `%w` 包装底层错误
- 常见错误：
  - `ErrNoDataPoints`：未找到数据点
  - `errInvalidPartition`：无效分区

### 并发控制

- 使用 `sync.Map` 存储指标，提供并发安全的访问
- 使用 `sync.RWMutex` 保护内存分区的数据点切片
- 使用 `sync.Once` 确保最小时间戳只设置一次
- 使用 `atomic` 操作确保数值的原子性
- 通过 channel 限制并发 worker 数量，防止资源耗尽

### 测试规范

- 每个源文件都有对应的测试文件
- 使用 `*_test.go` 命名测试文件
- 测试覆盖核心功能：插入、查询、分区管理、WAL 恢复等
- 基准测试使用 4 秒运行时间（`-benchtime=4s`）

### 配置选项

使用选项模式（Option Pattern）配置存储：

- `WithDataPath`：指定数据目录路径
- `WithPartitionDuration`：设置分区时长（默认 1 小时）
- `WithRetention`：设置数据保留时长（默认 14 天）
- `WithTimestampPrecision`：设置时间戳精度（默认纳秒）
- `WithWriteTimeout`：设置写入超时（默认 30 秒）
- `WithLogger`：设置日志记录器
- `WithWALBufferedSize`：设置 WAL 缓冲区大小（默认 4096 字节）

## 目录结构

```
tstorage/
├── storage.go              # 主存储实现
├── partition.go            # 分区接口定义
├── memory_partition.go     # 内存分区实现
├── disk_partition.go       # 磁盘分区实现
├── partition_list.go       # 分区列表管理
├── wal.go                  # WAL 接口定义
├── disk_wal.go             # 磁盘 WAL 实现
├── encoding.go             # 数据编码/解码
├── label.go                # 标签处理
├── logger.go               # 日志接口
├── bstream.go              # 位流处理
├── fake_encoder.go         # 测试用编码器
├── fake_partition.go       # 测试用分区
├── internal/               # 内部包
│   ├── cgroup/            # CPU 资源检测
│   ├── syscall/           # 系统调用封装
│   ├── timerpool/         # 定时器池
│   └── encoding/          # 内部编码
├── testdata/               # 测试数据
│   └── meta.json          # 示例元数据
├── go.mod                  # Go 模块定义
├── Makefile               # 构建和测试命令
├── README.md              # 项目文档
└── LICENSE                # 许可证
```

## 常见用例

### 基本使用

```go
import "github.com/nakabonne/tstorage"

// 创建内存存储
storage, _ := tstorage.NewStorage(
    tstorage.WithTimestampPrecision(tstorage.Seconds),
)
defer storage.Close()

// 插入数据
storage.InsertRows([]tstorage.Row{
    {
        Metric: "metric1",
        DataPoint: tstorage.DataPoint{Timestamp: 1600000000, Value: 0.1},
    },
})

// 查询数据
points, _ := storage.Select("metric1", nil, 1600000000, 1600000001)
```

### 持久化存储

```go
// 创建磁盘存储
storage, _ := tstorage.NewStorage(
    tstorage.WithDataPath("./data"),
    tstorage.WithPartitionDuration(2 * time.Hour),
    tstorage.WithRetention(7 * 24 * time.Hour),
)
defer storage.Close()
```

### 带标签的指标

```go
labels := []tstorage.Label{
    {Name: "host", Value: "host-1"},
}

storage.InsertRows([]tstorage.Row{
    {
        Metric:    "mem_alloc_bytes",
        Labels:    labels,
        DataPoint: tstorage.DataPoint{Timestamp: 1600000000, Value: 0.1},
    },
})
```

## 性能特性

- **写入性能**：基准测试显示约 305.9 ns/op，174 B/op，2 allocs/op
- **查询性能**：在百万级数据点中查询约 292.2 ns/op，56 B/op，1 allocs/op
- **内存优化**：使用内存映射减少内存占用
- **缓存友好**：内存分区使用有序切片，提供良好的缓存命中率

## 注意事项

1. **时间范围**：`Select` 方法的 start 参数是包含的，end 参数是排除的
2. **写入限制**：并发写入数受限于 CPU 核心数，可通过 cgroup 检测
3. **WAL 恢复**：启动时会自动从 WAL 恢复未持久化的数据
4. **分区切换**：当分区时间范围填满时，自动创建新分区
5. **过期清理**：定期检查并删除过期的磁盘分区
6. **乱序数据**：只保留在可写分区范围内的乱序数据，超出范围的数据会被丢弃

## 建议阅读路线

### 第一阶段：理解核心概念（1-2 个文件）

1. **doc.go** - 项目概述，了解包的用途
2. **partition.go** - 分区接口定义，理解分区的生命周期和基本操作

### 第二阶段：内存分区实现（2-3 个文件）

3. **memory_partition.go** - 内存分区的实现
   - 学习如何管理内存中的数据点
   - 理解有序切片的使用和乱序数据处理
   - 关注 `insertRows` 和 `selectDataPoints` 方法

4. **label.go** - 标签处理
   - 学习如何组合指标名称和标签

### 第三阶段：磁盘分区实现（1-2 个文件）

5. **disk_partition.go** - 磁盘分区的实现
   - 理解内存映射（mmap）的使用
   - 学习元数据结构和数据文件组织
   - 关注 `openDiskPartition` 和 `selectDataPoints` 方法

6. **encoding.go** - 数据编码/解码
   - 了解数据序列化和压缩方式

### 第四阶段：主存储引擎（1 个文件）

7. **storage.go** - 核心存储实现
   - 理解分区列表的管理
   - 学习分区切换和刷盘逻辑
   - 关注 `InsertRows`、`Select` 和 `flushPartitions` 方法

### 第五阶段：WAL 和持久化（1-2 个文件）

8. **wal.go** - WAL 接口定义
9. **disk_wal.go** - 磁盘 WAL 实现
   - 理解预写日志的写入和恢复机制

### 第六阶段：辅助组件（2-3 个文件）

10. **partition_list.go** - 分区列表管理
    - 学习如何有序管理多个分区

11. **internal/syscall/mmap.go** - 内存映射封装
    - 理解跨平台的 mmap 实现

### 第七阶段：测试和示例（可选）

12. **storage_test.go** - 主存储测试
13. **storage_examples_test.go** - 使用示例
14. **storage_benchmark_test.go** - 性能测试

### 阅读重点

**关键数据结构**：
- `Row` / `DataPoint` - 数据行和数据点
- `memoryPartition` / `diskPartition` - 分区实现
- `meta` / `diskMetric` - 磁盘分区元数据

**关键流程**：
- 数据写入流程：WAL → 内存分区 → 分区列表
- 数据查询流程：遍历分区 → 二分查找 → 返回结果
- 分区切换流程：检测 → 刷盘 → 创建新分区
- 恢复流程：读取 WAL → 重新插入数据

**设计模式**：
- 选项模式（Option Pattern）配置存储
- 接口隔离（Partition、Storage、Reader）
- 工厂模式创建分区和编码器

## 相关资源

- [Go 包文档](https://pkg.go.dev/mod/github.com/nakabonne/tstorage)
- [博客文章：从头编写时序数据库引擎](https://nakabonne.dev/posts/write-tsdb-from-scratch)
- [项目仓库](https://github.com/nakabonne/tstorage)