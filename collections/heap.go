package collections

type Less func(array *[]interface{}, i, j int) bool

type Heap struct {
	Less
	array      []interface{}
	lastOffset int
}

func Init(l Less) *Heap {
	return &Heap{
		Less:       l,
		array:      make([]interface{}, 1), // 创建一个index=0是空的数组
		lastOffset: 0,
	}
}

func (h *Heap) swap(i, j int) {
	h.array[i], h.array[j] = h.array[j], h.array[i]
}

// 向上浮动
// k当前元素在数组中的下标
func (h *Heap) swim(k int) {
	for k > 1 { // 当前节点还未到达父节点
		if h.Less(&h.array, k, k/2) {
			h.swap(k, k/2)
			k = k / 2
		} else {
			break
		}
	}
}

// 向下沉
func (h *Heap) sink(k int) {

	for k*2 <= h.lastOffset { // 当前节点还未到达底部
		j := k * 2
		if j+1 <= h.lastOffset {
			if h.Less(&h.array, j+1, j) {
				j = j + 1
			}
		}
		if h.Less(&h.array, j, k) {
			h.swap(k, j)
			k = j
		} else {
			break
		}
	}

}

func (h *Heap) Push(i interface{}) {
	h.array = append(h.array, i)
	h.lastOffset++
	h.swim(h.lastOffset)
}

func (h *Heap) Empty() bool {
	return h.lastOffset == 0
}

func (h *Heap) Pop() (i interface{}) {

	if h.Empty() {
		return nil
	}

	p := h.array[1]

	if h.lastOffset == 1 {
		h.lastOffset--
		return p
	}

	h.swap(1, h.lastOffset)
	h.array = h.array[:h.lastOffset]
	h.lastOffset--
	h.sink(1)
	return p
}

func (h *Heap) Clear() {
	h.array = nil
}
