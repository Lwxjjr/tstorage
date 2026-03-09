# GEMINI.md - tstorage Project Context

`tstorage` 是一个用 Go 编写的轻量级、本地磁盘时序数据库引擎。它专为高性能数据摄取而设计，通过按时间分区来管理数据。

## Project Overview

- **Core Technology:** Go 1.20+, 使用 `mmap` 进行磁盘数据映射，自定义时间轴分区架构。
- **Architecture:** 采用线性数据模型，将时序数据按时间范围分割成多个独立的“分区（Partition）”。
- **Data Flow:** 
    1. **Memory Partition (Active):** 数据首先写入内存分区，并同步记录到 WAL（预写日志）以保证可靠性。
    2. **WAL (Write-Ahead Log):** 在数据进入内存前记录，防止崩溃导致的数据丢失。
    3. **Disk Partition (ReadOnly):** 内存分区写满或过期后，会被压缩并持久化到磁盘，通过 `mmap` 实现高效读取。

## Building and Running

以下是常用的开发命令（基于 `Makefile`）：

- **运行测试:** `make test` (包含竞态检测和覆盖率)
- **运行基准测试:** `make test-bench` (生成 CPU 和内存 profile)
- **分析性能:**
    - 内存: `make pprof-mem`
    - CPU: `make pprof-cpu`
- **整理依赖:** `make dep`
- **查看文档:** `make godoc` (在本地 6060 端口启动)

## Recommended Reading Order (建议阅读顺序)

为了快速理解这个项目的设计思路，建议按以下顺序阅读源码：

1.  **`storage.go`**: 整个库的入口，定义了对外暴露的 `Storage` 和 `Reader` 接口。
2.  **`partition.go`**: 核心抽象接口。理解内存分区和磁盘分区是如何被统一对待的。
3.  **`partition_list.go`**: 了解分区是如何通过链表组织在一起的，以及如何进行新旧分区的切换。
4.  **`memory_partition.go`**: 深入了解热数据的摄取逻辑和乱序数据处理。
5.  **`wal.go` & `disk_wal.go`**: 学习数据的持久化安全机制。
6.  **`disk_partition.go`**: 了解冷数据如何落盘，以及 `mmap` 的具体使用方式。
7.  **`encoding.go` & `bstream.go`**: 探索底层的数据压缩算法和位流实现。

## Development Conventions

- **Partitioning:** 默认按 1 小时进行分区。
- **Concurrency:** 写入操作被限制在 `GOMAXPROCS` 以优化 CPU 密集型任务。
- **Disk Structure:** 持久化分区存储在以 `p-` 为前缀的目录下，包含 `meta.json` (元数据) 和 `data` (二进制数据)。
- **Errors:** 预定义了 `ErrNoDataPoints` 等错误，用于处理空查询。
