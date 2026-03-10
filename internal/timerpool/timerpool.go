package timerpool

import (
	"sync"
	"time"
)

var timerPool sync.Pool

// Get 从池中返回一个给定时长 d 的定时器。
//
// 使用 Put 将定时器归还到池中。
func Get(d time.Duration) *time.Timer {
	if v := timerPool.Get(); v != nil {
		t := v.(*time.Timer)
		// 复用旧的 Timer，并设置新的时间
		// 如果 Reset 返回 true，说明这个 Timer 还在跑
		// 正常情况下 Put 进去的 Timer 都应该是停止的
		if t.Reset(d) {
			panic("活跃的定时器被捕获到池中！")
		}
		return t
	}
	// 池子里没东西，才创建一个新的
	return time.NewTimer(d)
}

// Put 将 t 归还到池中。
//
// t 在归还到池中后不能再被访问。
func Put(t *time.Timer) {
	if !t.Stop() {
		// 如果 t.Stop() 返回 false，说明定时器已经到期了，
		// 它的通道 t.C 里可能已经塞进了一个时间值
		//当定时器到时间（Expired）时，它会往自己的通道 t.C里塞进一个时间戳
		select {
		case <-t.C: // 排空通道
		default:
		}
	}
	timerPool.Put(t)
}
