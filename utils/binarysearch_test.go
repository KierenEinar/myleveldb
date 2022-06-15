package utils

import (
	"myleveldb/comparer"
	"testing"
)

var intCmp = func(a, b interface{}) int {
	aInt := a.(int)
	bInt := b.(int)
	if aInt^bInt == 0 {
		return 0
	} else if aInt > bInt {
		return 1
	} else {
		return -1
	}
}

func TestBinarySearch(t *testing.T) {
	type args struct {
		arr []interface{}
		key interface{}
		cmp comparer.Compare
	}

	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "输入: [1, 3, 5, 7, 9], 查找 key=4, 输出: -3, 负数代表在数组中不存在该元素, 但是在下标索引 2",
			args: struct {
				arr []interface{}
				key interface{}
				cmp comparer.Compare
			}{
				arr: []interface{}{1, 3, 5, 7, 9},
				key: 4,
				cmp: intCmp,
			},
			want: -3,
		},
		{
			name: "输入: [1, 3, 5, 7, 9], 查找 key=12 输出: -6",
			args: struct {
				arr []interface{}
				key interface{}
				cmp comparer.Compare
			}{
				arr: []interface{}{1, 3, 5, 7, 9},
				key: 12,
				cmp: intCmp,
			},
			want: -6,
		},
		{
			name: "输入: [1, 3, 5, 7, 9], 查找 key=3  输出: 1",
			args: struct {
				arr []interface{}
				key interface{}
				cmp comparer.Compare
			}{
				arr: []interface{}{1, 3, 5, 7, 9},
				key: 3,
				cmp: intCmp,
			},
			want: 1,
		},
		{
			name: "输入: [1, 3, 5, 7, 9], 查找 key=-5 输出: -1",
			args: struct {
				arr []interface{}
				key interface{}
				cmp comparer.Compare
			}{
				arr: []interface{}{1, 3, 5, 7, 9},
				key: -5,
				cmp: intCmp,
			},
			want: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BinarySearch(tt.args.arr, tt.args.key, tt.args.cmp); got != tt.want {
				t.Errorf("BinarySearch() = %v, want %v", got, tt.want)
			}
		})
	}
}
