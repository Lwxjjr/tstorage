# tstorage 项目指南

本文档为 AI 助手提供了 tstorage 项目的上下文信息，以便更好地理解和参与项目开发。

## 项目概述

tstorage 是一个轻量级的本地磁盘时序数据存储引擎，提供简单的 API 和 goroutine 安全的写入与读取功能。该项目专为处理大量时序数据而优化，特别适合需要实时分析的应用场景。

### 主要特性

- **高性能写入**：针对大量时序数据摄取进行了优化
- **Goroutine 安全**：支持并发写入和读取
- **灵活存储**：支持内存存储和磁盘持久化
- **时间分区**：采用线性数据模型，按时间分区存储数据
- **WAL 支持**：预写日志（Write-Ahead Log）防止数据丢失
- **标签支持**：支持带标签的指标标识
- **乱序数据处理**：能够处理网络延迟或时钟同步导致的乱序数据点

### 技术栈

- **语言**：Go 1.20+
- **主要依赖**：
  - `github.com/stretchr/testify` - 测试框架
  - `github.com/davecgh/go-spew` - 数据转储
  - `gopkg.in/yaml.v3` - YAML 解析

### 项目架构

tstorage 采用基于时间分区的线性数据模型，与传统 B 树或 LSM 树存储引擎完全不同：

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

#### 核心组件

1. **Storage**：主存储接口，提供数据插入和查询功能
   - `InsertRows()`：插入数据行
   - `Select()`：查询指定时间范围的数据点
   - `Close()`：优雅关闭，刷新未写入的数据

2. **Partition**：分区接口，每个分区作为完全独立的数据库
   - **Memory Partition**：可写分区，存储在堆中，使用有序切片提供良好的缓存命中率
   - **Disk Partition**：只读分区，使用 mmap 内存映射，将数据持久化到磁盘

3. **WAL（Write-Ahead Log）**：预写日志，在数据插入内存分区前先写入日志，防止数据丢失

4. **PartitionList**：管理所有分区的列表，处理分区的创建、切换和过期

#### 数据模型

```go
type Row struct {
    Metric    string     // 指标名称
    Labels    []Label    // 可选标签
    DataPoint            // 数据点
}

type DataPoint struct {
    Value     float64    // 实际值
    Timestamp int64      // Unix 时间戳
}
```

### 磁盘存储结构

当启用磁盘持久化时，数据按以下结构存储：

```
./data
├── p-1600000001-1600003600/
│   ├── data              // 压缩的数据文件（mmap 映射）
│   └── meta.json         // 分区元数据
├── p-1600003601-1600007200/
│   ├── data
│   └── meta.json
└── wal/                  // 预写日志目录
```

## 构建和运行

### 环境要求

- Go 1.20 或更高版本

### 常用命令

```bash
# 运行测试（带竞态检测和覆盖率）
make test

# 运行基准测试
make test-bench

# 分析内存性能
make pprof-mem

# 分析 CPU 性能
make pprof-cpu

# 整理依赖
make dep

# 启动 godoc 文档服务器
make godoc
```

### 基本使用示例

```go
package main

import (
    "github.com/nakabonne/tstorage"
)

func main() {
    // 创建存储（默认内存模式）
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
}
```

### 启用磁盘持久化

```go
storage, _ := tstorage.NewStorage(
    tstorage.WithDataPath("./data"),  // 指定数据存储路径
)
defer storage.Close()
```

## 开发约定

### 代码风格

- **注释语言**：项目代码注释已翻译为中文
- **命名规范**：遵循 Go 语言命名约定
  - 接口名使用大写字母开头（如 `Storage`、`Partition`）
  - 私有字段和方法使用小写字母开头
  - 常量使用大写字母或驼峰命名

### 测试规范

- 使用标准 Go 测试框架
- 使用 `github.com/stretchr/testify` 进行断言
- 测试文件以 `_test.go` 结尾
- 测试命令包含竞态检测：`go test -race`
- 代码覆盖率目标：通过 `make test` 生成覆盖率报告

### 文件组织

```
tstorage/
├── storage.go              # 主存储接口和实现
├── partition.go            # 分区接口定义
├── memory_partition.go     # 内存分区实现
├── disk_partition.go       # 磁盘分区实现
├── wal.go                  # 预写日志接口和实现
├── disk_wal.go             # 磁盘 WAL 实现
├── partition_list.go       # 分区列表管理
├── encoding.go             # 数据编码/解码
├── label.go                # 标签处理
├── bstream.go              # 位流处理
├── logger.go               # 日志工具
├── internal/               # 内部包
│   ├── cgroup/            # cgroup 资源限制
│   ├── encoding/          # 内部编码
│   ├── syscall/           # 系统调用封装（mmap）
│   └── timerpool/         # 定时器池
└── testdata/              # 测试数据
```

### 核心概念

1. **分区生命周期**：可写 → 只读
   - 头部分区始终是可写的内存分区
   - 当分区填满时，转换为磁盘分区并持久化

2. **时间戳精度**：支持纳秒、微秒、毫秒、秒四种精度
   - 默认：纳秒
   - 可通过 `WithTimestampPrecision` 配置

3. **并发控制**：
   - 数据摄取的并发限制为 GOMAXPROCS（基于 cgroup 可用 CPU 数量）
   - 所有分区操作都是 goroutine 安全的

4. **数据压缩**：
   - 磁盘分区的数据文件经过压缩
   - 每个指标的数据点单独压缩，便于读取

### 配置选项

通过 `Option` 函数配置存储：

- `WithDataPath(string)` - 设置数据存储路径
- `WithTimestampPrecision(TimestampPrecision)` - 设置时间戳精度
- `WithPartitionDuration(time.Duration)` - 设置分区持续时间
- `WithRetention(time.Duration)` - 设置数据保留时间
- `WithWriteTimeout(time.Duration)` - 设置写入超时

### 性能优化建议

1. **写入优化**：
   - 批量插入数据（使用 `InsertRows`）
   - 避免频繁的小批量写入
   - 合理设置分区持续时间

2. **查询优化**：
   - 尽量缩小查询时间范围
   - 利用时间戳过滤减少数据扫描
   - 最近的数据通常在内存分区中，查询更快

3. **资源管理**：
   - 定期清理过期分区
   - 合理设置保留时间
   - 监控内存使用情况

## 常见任务

### 添加新的存储选项

1. 在 `storage.go` 中定义新的 Option 函数
2. 在 `storage` 结构体中添加对应字段
3. 在 `NewStorage` 中应用该选项

### 实现新的分区类型

1. 实现 `partition` 接口的所有方法
2. 在 `partition_list.go` 中注册新的分区类型
3. 添加相应的测试用例

### 修改编码格式

1. 修改 `encoding.go` 中的编码/解码逻辑
2. 更新 `meta.json` 的结构定义
3. 确保向后兼容性或提供迁移方案

## 相关资源

- **Go 文档**：https://pkg.go.dev/mod/github.com/nakabonne/tstorage
- **博客文章**：[Write a time-series database engine from scratch](https://nakabonne.dev/posts/write-tsdb-from-scratch)
- **灵感来源**：
  - https://misfra.me/state-of-the-state-part-iii
  - https://fabxc.org/tsdb
  - https://questdb.io/blog/2020/11/26/why-timeseries-data
  - https://akumuli.org/akumuli/2017/04/29/nbplustree
  - https://github.com/VictoriaMetrics/VictoriaMetrics

## 贡献指南

1. 确保代码通过所有测试：`make test`
2. 添加新的测试用例覆盖新功能
3. 更新相关文档
4. 遵循现有的代码风格和命名规范
5. 提交前运行 `make dep` 整理依赖

## 注意事项

- 所有公共 API 必须保持向后兼容
- 修改核心数据结构时需谨慎，考虑数据迁移
- 涉及磁盘操作的代码需正确处理错误
- 添加新功能时应提供相应的基准测试
- 注意内存泄漏和资源释放
- 确保 WAL 的正确性，避免数据丢失