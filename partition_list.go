package tstorage

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// partitionList 表示分区的链表。
// 每个分区按从最新到最旧的顺序排列。
// 也就是说，头节点始终是最新的，尾节点是最旧的。
//
// 头部及其下一个分区必须是可写的，以接受乱序数据点，
// 即使它是不活动的。
type partitionList interface {
	// insert 将一个新节点追加到头部。
	insert(partition partition)
	// remove 从列表中删除给定的分区。
	remove(partition partition) error
	// swap 用新分区替换旧分区。
	swap(old, new partition) error
	// getHead 返回作为最新节点的头节点。
	getHead() partition
	// size 返回它自己的分区数量。
	size() int
	// newIterator 返回此列表的迭代器对象。
	// 如果需要检查列表中的所有节点，请使用此方法。
	newIterator() partitionIterator

	String() string
}

// Iterator 表示分区列表的迭代器。基本用法如下：
/*
  for iterator.next() {
    partition, err := iterator.value()
    // 使用分区做些什么
  }
*/
type partitionIterator interface {
	// next 将迭代器定位到列表中的下一个节点。
	// 第一次调用时将定位到头部。
	// 如果可以从列表中读取值，则返回值为 true。
	next() bool
	// value 返回迭代器中的当前分区。
	// 即使在 next() 返回 false 时调用它，也会返回 nil。
	value() partition

	currentNode() *partitionNode
}

type partitionListImpl struct {
	numPartitions int64
	head          *partitionNode
	tail          *partitionNode
	mu            sync.RWMutex
}

func newPartitionList() partitionList {
	return &partitionListImpl{}
}

func (p *partitionListImpl) getHead() partition {
	if p.size() <= 0 {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.head.value()
}

func (p *partitionListImpl) insert(partition partition) {
	node := &partitionNode{
		val: partition,
	}
	p.mu.RLock()
	head := p.head
	p.mu.RUnlock()
	if head != nil {
		node.next = head
	}

	p.setHead(node)
	atomic.AddInt64(&p.numPartitions, 1)
}

func (p *partitionListImpl) remove(target partition) error {
	if p.size() <= 0 {
		return fmt.Errorf("empty partition")
	}

	// 从头开始遍历自身。
	var prev, next *partitionNode
	iterator := p.newIterator()
	for iterator.next() {
		current := iterator.currentNode()
		if !samePartitions(current.value(), target) {
			prev = current
			continue
		}

		// 删除当前节点。

		iterator.next()
		next = iterator.currentNode()
		switch {
		case prev == nil:
			// 删除头节点
			p.setHead(next)
		case next == nil:
			// 删除尾节点
			prev.setNext(nil)
			p.setTail(prev)
		default:
			// 删除中间节点
			prev.setNext(next)
		}
		atomic.AddInt64(&p.numPartitions, -1)

		if err := current.value().clean(); err != nil {
			return fmt.Errorf("failed to clean resources managed by partition to be removed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("the given partition was not found")
}

func (p *partitionListImpl) swap(old, new partition) error {
	if p.size() <= 0 {
		return fmt.Errorf("empty partition")
	}

	// 从头开始遍历自身。
	var prev, next *partitionNode
	iterator := p.newIterator()
	for iterator.next() {
		current := iterator.currentNode()
		if !samePartitions(current.value(), old) {
			prev = current
			continue
		}

		// 交换当前节点。

		newNode := &partitionNode{
			val:  new,
			next: current.getNext(),
		}
		iterator.next()
		next = iterator.currentNode()
		switch {
		case prev == nil:
			// 交换头节点
			p.setHead(newNode)
		case next == nil:
			// 交换尾节点
			prev.setNext(newNode)
			p.setTail(newNode)
		default:
			// swapping the middle node
			prev.setNext(newNode)
		}
		return nil
	}

	return fmt.Errorf("the given partition was not found")
}

func samePartitions(x, y partition) bool {
	return x.minTimestamp() == y.minTimestamp()
}

func (p *partitionListImpl) size() int {
	return int(atomic.LoadInt64(&p.numPartitions))
}

func (p *partitionListImpl) newIterator() partitionIterator {
	p.mu.RLock()
	head := p.head
	p.mu.RUnlock()
	// Put a dummy node so that it positions the head on the first next() call.
	dummy := &partitionNode{
		next: head,
	}
	return &partitionIteratorImpl{
		current: dummy,
	}
}

func (p *partitionListImpl) setHead(node *partitionNode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = node
}

func (p *partitionListImpl) setTail(node *partitionNode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tail = node
}

func (p *partitionListImpl) String() string {
	b := &strings.Builder{}
	iterator := p.newIterator()
	for iterator.next() {
		p := iterator.value()
		if _, ok := p.(*memoryPartition); ok {
			b.WriteString("[Memory Partition]")
		} else if _, ok := p.(*diskPartition); ok {
			b.WriteString("[Disk Partition]")
		} else {
			b.WriteString("[Unknown Partition]")
		}
		b.WriteString("->")
	}
	return strings.TrimSuffix(b.String(), "->")
}

// partitionNode wraps a partition to hold the pointer to the next one.
type partitionNode struct {
	// val is immutable
	val  partition
	next *partitionNode
	mu   sync.RWMutex
}

// value gives back the actual partition of the node.
func (p *partitionNode) value() partition {
	return p.val
}

func (p *partitionNode) setNext(node *partitionNode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next = node
}

func (p *partitionNode) getNext() *partitionNode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.next
}

type partitionIteratorImpl struct {
	current *partitionNode
}

func (i *partitionIteratorImpl) next() bool {
	if i.current == nil {
		return false
	}
	next := i.current.getNext()
	i.current = next
	return i.current != nil
}

func (i *partitionIteratorImpl) value() partition {
	if i.current == nil {
		return nil
	}
	return i.current.value()
}

func (i *partitionIteratorImpl) currentNode() *partitionNode {
	return i.current
}
