package comparer

// Compare 比较
// 当a元素小于b的时候, 返回-1
// 相等时返回 0
// 当a元素大于b的时候, 返回-1
type Compare func(a, b interface{}) int
