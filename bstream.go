// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package tstorage

import (
	"encoding/binary"
	"io"
)

// bstream 是一个位流。
type bstream struct {
	stream []byte // 数据流
	count  uint8  // 当前字节中有多少位是有效的
}

func (b *bstream) bytes() []byte {
	return b.stream
}

// reset 将缓冲区重置为空，
// 但它保留底层存储以供将来写入使用。
func (b *bstream) reset() {
	b.stream = b.stream[:0]
	b.count = 0
}

type bit bool

const (
	zero bit = false
	one  bit = true
)

func (b *bstream) writeBit(bit bit) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1

	if bit {
		b.stream[i] |= 1 << (b.count - 1)
	}

	b.count--
}

func (b *bstream) writeByte(byt byte) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1

	// fill up b.b with b.count bits from byt
	b.stream[i] |= byt >> (8 - b.count)

	b.stream = append(b.stream, 0)
	i++
	b.stream[i] = byt << b.count
}

func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= (64 - uint(nbits))
	for nbits >= 8 {
		byt := byte(u >> 56)
		b.writeByte(byt)
		u <<= 8
		nbits -= 8
	}

	for nbits > 0 {
		b.writeBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

type bstreamReader struct {
	stream       []byte
	streamOffset int // 从流中读取下一个字节的偏移量。

	buffer uint64 // 当前缓冲区，从流中填充，包含多达 8 个字节，用于读取位。
	valid  uint8  // 当前缓冲区中有效读取的位数（从左侧开始）。
}

func newBReader(b []byte) bstreamReader {
	return bstreamReader{
		stream: b,
	}
}

func (b *bstreamReader) readBit() (bit, error) {
	if b.valid == 0 {
		if !b.loadNextBuffer(1) {
			return false, io.EOF
		}
	}

	return b.readBitFast()
}

// readBitFast 类似于 readBit，但如果内部缓冲区为空，可以返回 io.EOF。
	// 如果返回 io.EOF，调用者应该重试读取位，调用 readBit()。
	// 这个函数必须保持小且作为叶子函数，以帮助编译器内联它
	// 并进一步提高性能。
func (b *bstreamReader) readBitFast() (bit, error) {
	if b.valid == 0 {
		return false, io.EOF
	}

	b.valid--
	bitmask := uint64(1) << b.valid
	return (b.buffer & bitmask) != 0, nil
}

func (b *bstreamReader) readBits(nbits uint8) (uint64, error) {
	if b.valid == 0 {
		if !b.loadNextBuffer(nbits) {
			return 0, io.EOF
		}
	}

	if nbits <= b.valid {
		return b.readBitsFast(nbits)
	}

	// 我们必须从当前缓冲区读取所有剩余的有效位，并从下一个缓冲区读取一部分。
	bitmask := (uint64(1) << b.valid) - 1
	nbits -= b.valid
	v := (b.buffer & bitmask) << nbits
	b.valid = 0

	if !b.loadNextBuffer(nbits) {
		return 0, io.EOF
	}

	bitmask = (uint64(1) << nbits) - 1
	v = v | ((b.buffer >> (b.valid - nbits)) & bitmask)
	b.valid -= nbits

	return v, nil
}

// readBitsFast 类似于 readBits，但如果内部缓冲区为空，可以返回 io.EOF。
	// 如果返回 io.EOF，调用者应该重试读取位，调用 readBits()。
	// 这个函数必须保持小且作为叶子函数，以帮助编译器内联它
	// 并进一步提高性能。
func (b *bstreamReader) readBitsFast(nbits uint8) (uint64, error) {
	if nbits > b.valid {
		return 0, io.EOF
	}

	bitmask := (uint64(1) << nbits) - 1
	b.valid -= nbits

	return (b.buffer >> b.valid) & bitmask, nil
}

func (b *bstreamReader) ReadByte() (byte, error) {
	v, err := b.readBits(8)
	if err != nil {
		return 0, err
	}
	return byte(v), nil
}

// loadNextBuffer 从流中将下一个字节加载到内部缓冲区中。
	// 输入 nbits 是必须读取的最小位数，但实现
	// 可以读取更多（如果可能）以提高性能。
func (b *bstreamReader) loadNextBuffer(nbits uint8) bool {
	if b.streamOffset >= len(b.stream) {
		return false
	}

	// 以优化的方式处理缓冲区中有超过 8 个字节的情况（最常见的情况）。
	// 保证这个分支永远不会从流的最后一个字节读取（由于并发写入，这会遭受竞争条件）。
	if b.streamOffset+8 < len(b.stream) {
		b.buffer = binary.BigEndian.Uint64(b.stream[b.streamOffset:])
		b.streamOffset += 8
		b.valid = 64
		return true
	}

	// 如果流中剩余 8 个或更少的字节，我们就在这里。由于此读取器需要
	// 处理与在最后一个字节上发生的并发写入的竞争条件，
	// 我们确保永远不会读取超过请求的最小位数（四舍五入到下一个字节）。
	// 以下代码较慢，但调用频率较低。
	nbytes := int((nbits / 8) + 1)
	if b.streamOffset+nbytes > len(b.stream) {
		nbytes = len(b.stream) - b.streamOffset
	}

	buffer := uint64(0)
	for i := 0; i < nbytes; i++ {
		buffer = buffer | (uint64(b.stream[b.streamOffset+i]) << uint(8*(nbytes-i-1)))
	}

	b.buffer = buffer
	b.streamOffset += nbytes
	b.valid = uint8(nbytes * 8)

	return true
}
