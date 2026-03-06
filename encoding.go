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
	"fmt"
	"io"
	"math"
	"math/bits"
)

type seriesEncoder interface {
	encodePoint(point *DataPoint) error
	flush() error
}

func newSeriesEncoder(w io.Writer) seriesEncoder {
	return &gorillaEncoder{
		w:   w,
		buf: &bstream{stream: make([]byte, 0)},
	}
}

// gorillaEncoder 实现 Gorilla 的时序数据压缩。
	// 参见：http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
type gorillaEncoder struct {
	// 后端流写入器
	w io.Writer

	// 编码时使用的缓冲区
	buf *bstream

	// 计算增量差值：
	// D = (t_n − t_n−1) − (t_n−1 − t_n−2)
	//
	// t_0，起始时间戳 t_0
	// 不可变
	t0 int64
	// t_1，下一个起始时间戳
	// 不可变
	t1 int64
	// t_n，第 N 个数据点的时间戳
	// 可变
	t int64
	// t_n 的增量
	tDelta uint64

	// v_n，第 N 个数据点的值
	v        float64
	leading  uint8
	trailing uint8
}

// encodePoints 不是 goroutine 安全的。调用者有责任锁定它。
func (e *gorillaEncoder) encodePoint(point *DataPoint) error {
	var tDelta uint64

	// 借鉴自 https://github.com/prometheus/prometheus/blob/39d79c3cfb86c47d6bc06a9e9317af582f1833bb/tsdb/chunkenc/xor.go#L150
	switch {
	case e.t0 == 0:
		// 直接写入时间戳。
		buf := make([]byte, binary.MaxVarintLen64)
		for _, b := range buf[:binary.PutVarint(buf, point.Timestamp)] {
			e.buf.writeByte(b)
		}
		// 直接写入值。
		e.buf.writeBits(math.Float64bits(point.Value), 64)
		e.t0 = point.Timestamp
	case e.t1 == 0:
		// 写入时间戳的增量。
		tDelta = uint64(point.Timestamp - e.t0)

		buf := make([]byte, binary.MaxVarintLen64)
		for _, b := range buf[:binary.PutUvarint(buf, tDelta)] {
			e.buf.writeByte(b)
		}
		// 写入值的增量。
		e.writeVDelta(point.Value)
		e.t1 = point.Timestamp
	default:
		// 写入时间戳的增量差值。
		tDelta = uint64(point.Timestamp - e.t)
		deltaOfDelta := int64(tDelta - e.tDelta)
		switch {
		case deltaOfDelta == 0:
			e.buf.writeBit(zero)
		case -63 <= deltaOfDelta && deltaOfDelta <= 64:
			e.buf.writeBits(0x02, 2) // '10'
			e.buf.writeBits(uint64(deltaOfDelta), 7)
		case -255 <= deltaOfDelta && deltaOfDelta <= 256:
			e.buf.writeBits(0x06, 3) // '110'
			e.buf.writeBits(uint64(deltaOfDelta), 9)
		case -2047 <= deltaOfDelta && deltaOfDelta <= 2048:
			e.buf.writeBits(0x0e, 4) // '1110'
			e.buf.writeBits(uint64(deltaOfDelta), 12)
		default:
			e.buf.writeBits(0x0f, 4) // '1111'
			e.buf.writeBits(uint64(deltaOfDelta), 64)
		}
		// 写入值的增量。
		e.writeVDelta(point.Value)
	}

	e.t = point.Timestamp
	e.v = point.Value
	e.tDelta = tDelta
	return nil
}

// flush 将缓冲的字节写入后端 io.Writer
	// 并重置所有用于计算的内容。
func (e *gorillaEncoder) flush() error {
	// TODO: 使用 ZStandard 压缩
	_, err := e.w.Write(e.buf.bytes())
	if err != nil {
		return fmt.Errorf("failed to flush buffered bytes: %w", err)
	}

	e.buf.reset()
	e.t0 = 0
	e.t1 = 0
	e.t = 0
	e.tDelta = 0
	e.v = 0
	e.v = 0
	e.leading = 0
	e.trailing = 0

	return nil
}

func (e *gorillaEncoder) writeVDelta(v float64) {
	vDelta := math.Float64bits(v) ^ math.Float64bits(e.v)

	if vDelta == 0 {
		e.buf.writeBit(zero)
		return
	}
	e.buf.writeBit(one)

	leading := uint8(bits.LeadingZeros64(vDelta))
	trailing := uint8(bits.TrailingZeros64(vDelta))

	// Clamp number of leading zeros to avoid overflow when encoding.
	if leading >= 32 {
		leading = 31
	}

	if e.leading != 0xff && leading >= e.leading && trailing >= e.trailing {
		e.buf.writeBit(zero)
		e.buf.writeBits(vDelta>>e.trailing, 64-int(e.leading)-int(e.trailing))
	} else {
		e.leading, e.trailing = leading, trailing

		e.buf.writeBit(one)
		e.buf.writeBits(uint64(leading), 5)

		// 注意，如果 leading == trailing == 0，则 sigbits == 64。但是该值实际上不适合我们要有的 6 位。
		// 幸运的是，我们永远不需要编码 0 个有效位，因为那会使我们进入另一种情况（vdelta == 0）。
		// 所以我们写入一个 0 并在解包时将其调整回 64。
		sigbits := 64 - leading - trailing
		e.buf.writeBits(uint64(sigbits), 6)
		e.buf.writeBits(vDelta>>trailing, int(sigbits))
	}
}

type seriesDecoder interface {
	decodePoint(dst *DataPoint) error
}

// newSeriesDecoder 从给定的 Reader 解压缩数据，然后保存解压缩的数据
func newSeriesDecoder(r io.Reader) (seriesDecoder, error) {
	// TODO: 停止复制整个字节，然后使可以从 io.Reader 创建 bstreamReader
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read all bytes: %w", err)
	}
	return &gorillaDecoder{
		br: newBReader(b),
	}, nil
}

type gorillaDecoder struct {
	br      bstreamReader
	numRead uint16

	// 第 N 个数据点的时间戳
	t      int64
	tDelta uint64

	// 第 N 个数据点的值
	v        float64
	leading  uint8
	trailing uint8
}

func (d *gorillaDecoder) decodePoint(dst *DataPoint) error {
	if d.numRead == 0 {
		t, err := binary.ReadVarint(&d.br)
		if err != nil {
			return fmt.Errorf("failed to read Timestamp of T0: %w", err)
		}
		v, err := d.br.readBits(64)
		if err != nil {
			return fmt.Errorf("failed to read Value of T0: %w", err)
		}
		d.t = t
		d.v = math.Float64frombits(v)

		d.numRead++
		dst.Timestamp = d.t
		dst.Value = d.v
		return nil
	}
	if d.numRead == 1 {
		tDelta, err := binary.ReadUvarint(&d.br)
		if err != nil {
			return err
		}
		d.tDelta = tDelta
		d.t = d.t + int64(d.tDelta)

		if err := d.readValue(); err != nil {
			return err
		}
		d.numRead++
		dst.Timestamp = d.t
		dst.Value = d.v
		return nil
	}

	var delimiter byte

	

		// 读取增量差值

		for i := 0; i < 4; i++ {

			delimiter <<= 1

			bit, err := d.br.readBitFast()

			if err != nil {

				bit, err = d.br.readBit()

			}

			if err != nil {

				return err

			}

			if bit == zero {

				break

			}

			delimiter |= 1

		}

		var sz uint8

		var deltaOfDelta int64

		switch delimiter {

		case 0x00:

			// deltaOfDelta == 0

		case 0x02:

			sz = 7

		case 0x06:

			sz = 9

		case 0x0e:

			sz = 12

		case 0x0f:

			// 不使用快速读取，因为不太可能成功。

			bits, err := d.br.readBits(64)

			if err != nil {

				return err

			}

	

			deltaOfDelta = int64(bits)

		default:

			return fmt.Errorf("unknown delimiter found: %v", delimiter)

		}

	if sz != 0 {
		bits, err := d.br.readBitsFast(sz)
		if err != nil {
			bits, err = d.br.readBits(sz)
		}
		if err != nil {
			return err
		}
		if bits > (1 << (sz - 1)) {
			// or something
			bits = bits - (1 << sz)
		}
		deltaOfDelta = int64(bits)
	}

	d.tDelta = uint64(int64(d.tDelta) + deltaOfDelta)
	d.t = d.t + int64(d.tDelta)

	if err := d.readValue(); err != nil {
		return err
	}
	dst.Timestamp = d.t
	dst.Value = d.v
	return nil
}

func (d *gorillaDecoder) readValue() error {
	bit, err := d.br.readBitFast()
	if err != nil {
		bit, err = d.br.readBit()
	}
	if err != nil {
		return err
	}

	if bit == zero {
		// d.val = d.val
	} else {
		bit, err := d.br.readBitFast()
		if err != nil {
			bit, err = d.br.readBit()
		}
		if err != nil {
			return err
		}
		if bit == zero {
			// 重用前导/尾随零位
			// d.leading, d.trailing = d.leading, d.trailing
		} else {
			bits, err := d.br.readBitsFast(5)
			if err != nil {
				bits, err = d.br.readBits(5)
			}
			if err != nil {
				return err
			}
			d.leading = uint8(bits)

			bits, err = d.br.readBitsFast(6)
			if err != nil {
				bits, err = d.br.readBits(6)
			}
			if err != nil {
				return err
			}
			mbits := uint8(bits)
			// 这里的 0 个有效位意味着我们溢出了，实际上需要 64 位；参见编码器中的注释
			if mbits == 0 {
				mbits = 64
			}
			d.trailing = 64 - d.leading - mbits
		}

		mbits := 64 - d.leading - d.trailing
		bits, err := d.br.readBitsFast(mbits)
		if err != nil {
			bits, err = d.br.readBits(mbits)
		}
		if err != nil {
			return err
		}
		vbits := math.Float64bits(d.v)
		vbits ^= bits << d.trailing
		d.v = math.Float64frombits(vbits)
	}

	return nil
}

func bitRange(x int64, nbits uint8) bool {
	return -((1<<(nbits-1))-1) <= x && x <= 1<<(nbits-1)
}
