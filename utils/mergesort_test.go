package utils

import (
	"math/rand"
	"myleveldb/comparer"
	"testing"
)

func TestMergeSort(t *testing.T) {
	type args struct {
		arr []interface{}
		cmp comparer.Compare
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "正序输入, 期待输出 [2, 4, 5, 7, 9]",
			args: args{
				arr: []interface{}{4, 9, 7, 2, 5},
				cmp: intCmp,
			},
		},
		{
			name: "随机数, 正序输出",
			args: args{
				arr: randArr(),
				cmp: intCmp,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			MergeSort(tt.args.arr, tt.args.cmp)
			t.Logf("%#v", tt.args.arr)
		})
	}
}

func randArr() []interface{} {
	arr := make([]interface{}, 1e5)
	for idx := 0; idx < 1e5; idx++ {
		arr[idx] = rand.Intn(1 << 32)
	}
	return arr
}
