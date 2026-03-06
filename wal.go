package tstorage

import (
	"os"
	"sync"
)

type walOperation byte

const (
	// operateInsert 的记录格式如下所示：
	/*
	   +--------+---------------------+--------+--------------------+----------------+
	   | op(1b) | len metric(varints) | metric | timestamp(varints) | value(varints) |
	   +--------+---------------------+--------+--------------------+----------------+
	*/
	operationInsert walOperation = iota
)

// wal 表示预写日志，提供持久性保证。
type wal interface {
	append(op walOperation, rows []Row) error
	flush() error
	punctuate() error
	removeOldest() error
	removeAll() error
	refresh() error
}

type nopWAL struct {
	filename string
	f        *os.File
	mu       sync.Mutex
}

func (f *nopWAL) append(_ walOperation, _ []Row) error {
	return nil
}

func (f *nopWAL) flush() error {
	return nil
}

func (f *nopWAL) punctuate() error {
	return nil
}

func (f *nopWAL) removeOldest() error {
	return nil
}

func (f *nopWAL) removeAll() error {
	return nil
}

func (f *nopWAL) refresh() error {
	return nil
}
