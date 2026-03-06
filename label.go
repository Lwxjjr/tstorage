package tstorage

import (
	"sort"

	"github.com/nakabonne/tstorage/internal/encoding"
)

const (
	// 标签名称的最大长度。
	//
	// 更长的名称会被截断。
	maxLabelNameLen = 256

	// 标签值的最大长度。
	//
	// 更长的值会被截断。
	maxLabelValueLen = 16 * 1024
)

// Label 是一个时序标签。
	// 缺少名称或值的标签是无效的。
type Label struct {
	Name  string
	Value string
}

// marshalMetricName 通过编码标签来构建唯一的字节。
func marshalMetricName(metric string, labels []Label) string {
	if len(labels) == 0 {
		return metric
	}
	invalid := func(name, value string) bool {
		return name == "" || value == ""
	}

	// 预先确定字节大小。
	size := len(metric) + 2
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})
	for i := range labels {
		label := &labels[i]
		if invalid(label.Name, label.Value) {
			continue
		}
		if len(label.Name) > maxLabelNameLen {
			label.Name = label.Name[:maxLabelNameLen]
		}
		if len(label.Value) > maxLabelValueLen {
			label.Value = label.Value[:maxLabelValueLen]
		}
		size += len(label.Name)
		size += len(label.Value)
		size += 4
	}

	// 开始构建字节。
	out := make([]byte, 0, size)
	out = encoding.MarshalUint16(out, uint16(len(metric)))
	out = append(out, metric...)
	for i := range labels {
		label := &labels[i]
		if invalid(label.Name, label.Value) {
			continue
		}
		out = encoding.MarshalUint16(out, uint16(len(label.Name)))
		out = append(out, label.Name...)
		out = encoding.MarshalUint16(out, uint16(len(label.Value)))
		out = append(out, label.Value...)
	}
	return string(out)
}
