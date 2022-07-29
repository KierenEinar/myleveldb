package utils

// MergeSort 归并排序
/**
*							+-----+-----+-----+-----+-----+
*							|  4  |  9  |  7  |  2  |  5  |
*							+-----+-----+-----+-----+-----+
*									/			\
*			+-----+-----+								+-----+-----+-----+
*			|  4  |  9  |								|  7  |  2  |  5  |
*			+-----+-----+								+-----+-----+-----+
*				/   \										/		   \
*		+-----+		  +-----+					+-----+						+-----+-----+
*		|  4  |		  |  9  |					|  7  |						|  2  |  5  |
*		+-----+		  +-----+					+-----+						+-----+-----+
*			\			/						   |							/	\
*			+-----+-----+						+-----+					+-----+		   +-----+
*			|  4  |  9  |						|  7  |					|  2  |		   |  5  |
*			+-----+-----+						+-----+					+-----+		   +-----+
*																			\			  /
*																			  +-----+-----+
*																			  |  2  |  5  |
*																			  +-----+-----+
*													/							 \
*														+-----+-----+-----+
*														|  2  |  5  |  7  |
*														+-----+-----+-----+
*								\			/
*						+-----+-----+-----+-----+-----+
*						|  2  |  4  |  5  |  7  |  9  |
*						+-----+-----+-----+-----+-----+
 */
func MergeSort(arr []interface{}, cmp Compare) {
	aux := make([]interface{}, len(arr))
	copy(aux, arr)
	mergeSort(arr, aux, 0, len(arr)-1, cmp)
}

func mergeSort(arr, aux []interface{}, lo int, hi int, cmp Compare) {

	if lo >= hi {
		return
	}

	mid := (lo + hi) >> 1
	mergeSort(arr, aux, lo, mid, cmp)
	mergeSort(arr, aux, mid+1, hi, cmp)
	merge(arr, aux, lo, mid, hi, cmp)
}

func merge(arr, aux []interface{}, lo, mid, hi int, cmp Compare) {

	i := lo
	j := mid + 1
	ptr := lo // 当前数组的指针
	for ptr <= hi {
		if j > hi {
			arr[ptr] = aux[i]
			i++
		} else if i > mid {
			arr[ptr] = aux[j]
			j++
		} else if cmp(aux[i], aux[j]) < 0 {
			arr[ptr] = aux[i]
			i++
		} else {
			arr[ptr] = aux[j]
			j++
		}
		ptr++
	}

	for ptr := lo; ptr <= hi; ptr++ {
		aux[ptr] = arr[ptr]
	}

}
