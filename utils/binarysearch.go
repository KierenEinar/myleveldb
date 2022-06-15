package utils

import "myleveldb/comparer"

// BinarySearch 二分搜索, 类似于java的collections.binarysearch()
// 因为用不惯golang的sort.Search
/***
e.g
输入: [1, 3, 5, 7, 9], 查找 key=4, 输出: -3, 负数代表在数组中不存在该元素, 但是在下标索引 2
输入: [1, 3, 5, 7, 9], 查找 key=12 输出: -6
输入: [1, 3, 5, 7, 9], 查找 key=3  输出: 1
输入: [1, 3, 5, 7, 9], 查找 key=-5 输出: -1
结论: 当且仅当找到元素的时候, 返回下标索引
	 否则, 返回 -(下标索引+1)

输入的数组必须保证是有序的, 时间复杂度为Olog(n)


					+-----+-----+-----+-----+-----+
					|  1  |  3  |  5  |  7  |  9  |
					+-----+-----+-----+-----+-----+
seek: 4
lo,mid,hi = 0, 2, 4
arr[mid] = 5 > 4
lo = 0, hi = 2
					+-----+-----+-----+
					|  1  |  3  |  5  |
					+-----+-----+-----+

lo,mid,hi = 0, 1, 2
arr[mid] = 3 < 4
lo = 1, hi = 2
					+-----+-----+
					|  3  |  5  |
					+-----+-----+

lo,mid,hi=1,1,2
arr[mid] = 3 < 4
lo = 2, hi = 2
					+-----+
					|  5  |
					+-----+
lo = 2, hi = 1

return -(2+1)


*/

func BinarySearch(arr []interface{}, key interface{}, cmp comparer.Compare) int {
	lo := 0
	hi := len(arr) - 1
	for lo <= hi {
		j := int(uint64(lo+hi) >> 1)
		r := cmp(arr[j], key)
		if r == 0 {
			return j
		} else if r < 0 {
			lo = j + 1
		} else {
			hi = j - 1
		}
	}
	return -(lo + 1)
}
