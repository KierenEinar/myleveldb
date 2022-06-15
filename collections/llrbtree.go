package collections

import (
	"errors"
	"fmt"
	"myleveldb/comparer"
	"myleveldb/utils"
	"sync"
	"sync/atomic"
)

/**
左倾红黑树满足以下几点特征
1. 新加入的节点都是默认红色
2. 根节点是黑色的
3. 红色节点必须在左侧
4. 由于遵照2-3树, 所以不能左侧不能出现多于一个的红色链接

添加所有用例

/  代表黑色链接
// 代表红色链接

				      (1)					     (2)
                     ----						 ----
					|    |						|    |
					 ----						 ----				进行一次左旋
				 //									   \\			--------->
				// 									    \\
			 ----	 									 ----
			|  n |										|  n  |
			 ----										 ----
当前无需做任何操作


					  (3)
			         ----
				    |    |
					 ----					 进行一次颜色变换操作, 父节点颜色相反
				  //	  \\				------------------------------>
				 //		   \\
		       ----	         ----
			  |    |        |  n  |
			   ----	         ----


  					  (4)
			         ----
				    |    |
					 ----				  进行一次右旋, 再进行一次颜色变换, 回到(3)
				  //	   \				-------------------------------------->
				 //		    \
		       ----	         ----
			  |    |        |    |
			   ----	         ----
			 //
			//
		   ----
		  |  n  |
		   ----

					 (5)
			         ----
				    |    |
					 ----				    左旋，回到(4)
				  //	  \				-------------------------------------->
				 //		   \
		       ----	         ----
			  |    |        |    |
			   ----	         ----
			 		\\
					 \\
					  ----
		  			|  n  |
					  ----



**/

// Common errors.
var (
	ErrIterReleased = errors.New("leveldb/llrbtree: iterator released")
	ErrCapFull      = errors.New("leveldb/llrbtree: capacity full")
	ErrClosed       = errors.New("leveldb/llrbtree: closed")
)

type color bool

const (
	black color = false
	red   color = true
)

func (c color) isRed() bool {
	return c == red
}

// LLRBTree 左倾红黑树
type LLRBTree struct {
	rw       sync.RWMutex
	root     *lLRBTReeNode
	ref      int32 // 被引用的次数
	released bool
	cmp      comparer.Compare
	data     []byte
	size     int
	pos      int
	capacity int
	pool     *utils.BytePool
}

// NewLLRBTree 实例化rbtree
// note, 需要手动Close方法才能释放llrbtree, 否则常驻内存
func NewLLRBTree(capacity int, cmp comparer.Compare, pool *utils.BytePool) *LLRBTree {
	tree := new(LLRBTree)
	tree.capacity = capacity
	tree.cmp = cmp
	tree.data = pool.Get(int64(capacity))
	tree.ref = 1
	tree.pool = pool
	return tree
}

type lLRBTReeNode struct {
	//key   []byte
	//value []byte
	color
	left   *lLRBTReeNode
	right  *lLRBTReeNode
	parent *lLRBTReeNode
	nodeKvIndex
}

// 存放node kv的信息
type nodeKvIndex struct {
	kPosition int
	kLen      int
	vPosition int
	vLen      int
}

func (nodeKvIndex nodeKvIndex) key(data []byte) []byte {
	return data[nodeKvIndex.kPosition : nodeKvIndex.kPosition+nodeKvIndex.kLen]
}

func (nodeKvIndex nodeKvIndex) value(data []byte) []byte {
	return data[nodeKvIndex.vPosition : nodeKvIndex.vPosition+nodeKvIndex.vLen]
}

func (lLRBTReeNode *lLRBTReeNode) isRed() bool {
	if lLRBTReeNode == nil {
		return false
	}
	return lLRBTReeNode.color.isRed()
}

// Ref 增加引用次数
func (rbTree *LLRBTree) Ref() {
	atomic.AddInt32(&rbTree.ref, 1)
}

// UnRef 减少引用次数
func (rbTree *LLRBTree) UnRef() {
	if ref := atomic.AddInt32(&rbTree.ref, -1); ref == 0 {
		fmt.Printf("rbtree close ... \n")
		rbTree.reset()
	}
}

func (rbTree *LLRBTree) reset() {
	rbTree.rw.Lock()
	defer rbTree.rw.Unlock()
	if !rbTree.released {
		rbTree.released = true
		rbTree.ref = 0
		rbTree.pos = 0
		rbTree.data = rbTree.data[:0]
		rbTree.pool.Put(rbTree.data)
		rbTree.size = 0
	}
}

// Put 写入记录, 采用for循环写入
func (rbTree *LLRBTree) Put(key, value []byte) error {

	rbTree.Ref()
	defer rbTree.UnRef()

	rbTree.rw.Lock()
	defer rbTree.rw.Unlock()

	kvLen := len(key) + len(value)
	if rbTree.capacity-kvLen < 0 {
		return ErrCapFull
	}
	rbTree.size += kvLen
	rbTree.put(key, value)
	rbTree.root.color = black
	return nil
}

// Set 写入记录, 采用递归方式写入
func (rbTree *LLRBTree) Set(key, value []byte) error {

	rbTree.Ref()
	defer rbTree.UnRef()

	rbTree.rw.Lock()
	defer rbTree.rw.Unlock()

	kvLen := len(key) + len(value)
	if rbTree.capacity-kvLen < 0 {
		return ErrCapFull
	}
	rbTree.size += kvLen

	kPos := rbTree.pos
	n := copy(rbTree.data[kPos:], key)
	kvIndex := nodeKvIndex{
		kPosition: kPos,
		kLen:      n,
		vPosition: kPos + n,
		vLen:      len(value),
	}
	m := copy(rbTree.data[kvIndex.vPosition:], value)
	rbTree.pos += n + m

	rbTree.root = rbTree.root.put(rbTree.cmp, key, value, &kvIndex, rbTree.data)
	rbTree.root.color = black
	return nil
}

func (rbTree *LLRBTree) put(key, value []byte) {

	kPos := rbTree.pos

	n := copy(rbTree.data[kPos:], key)

	kvIndex := nodeKvIndex{
		kPosition: kPos,
		kLen:      n,
		vPosition: kPos + n,
		vLen:      len(value),
	}

	m := copy(rbTree.data[kvIndex.vPosition:], value)

	rbTree.pos += n + m

	x := rbTree.root
	cmp := rbTree.cmp
	var parent *lLRBTReeNode
	var compare int
	var node *lLRBTReeNode
	// 从上往下查找, 位置对了就添加上去
	for {
		if x == nil {
			node = &lLRBTReeNode{
				//key:         key,
				//value:       value,
				color:       red,
				parent:      parent,
				nodeKvIndex: kvIndex,
			}
			// parent 是空, 肯定是根节点为空的情况
			if parent == nil {
				rbTree.root = node
				break
			}
			if compare > 0 {
				parent.left = node
				break
			}
			parent.right = node
			break
		}
		parent = x
		compare = cmp(x.key(rbTree.data), key)
		if compare > 0 {
			x = x.left
		} else if compare < 0 {
			x = x.right
		} else {
			x.nodeKvIndex = kvIndex
			return
		}

	}

	// 从下往上
	for {
		if parent == nil {
			break
		}
		if !parent.left.isRed() && parent.right.isRed() {
			parent = rotateLeft(parent)
			exchangeParent(cmp, parent, rbTree)
		} else {
			if parent.left.isRed() && parent.left.left.isRed() {
				parent = rotateRight(parent)
				exchangeParent(cmp, parent, rbTree)
			}
			if parent.left.isRed() && parent.right.isRed() {
				flipColor(parent)
				if parent.parent == nil {
					rbTree.root = parent
				}
			}
		}
		parent = parent.parent
	}

}

// 递归式的添加, 由于需要开栈、性能不如非递归版本, 但是实现相对简单
func (rbTreeNode *lLRBTReeNode) put(cmp comparer.Compare, key, value []byte, kvIndex *nodeKvIndex, data []byte) *lLRBTReeNode {
	if rbTreeNode == nil {
		return &lLRBTReeNode{
			nodeKvIndex: *kvIndex,
			color:       red,
		}
	}

	compare := cmp(rbTreeNode.key(data), key)

	if compare > 0 {
		rbTreeNode.left = rbTreeNode.left.put(cmp, key, value, kvIndex, data)
	} else if compare < 0 {
		rbTreeNode.right = rbTreeNode.right.put(cmp, key, value, kvIndex, data)
	} else {
		rbTreeNode.nodeKvIndex = *kvIndex
	}

	// 自底向上
	x := rbTreeNode
	if x.right.isRed() && !x.left.isRed() {
		x = rotateLeft(x)
	} else {
		if x.left.isRed() && x.left.left.isRed() {
			x = rotateRight(x)
		}

		if x.left.isRed() && x.right.isRed() {
			flipColor(x)
		}

	}

	return x
}

// 左旋
func rotateLeft(h *lLRBTReeNode) *lLRBTReeNode {
	right := h.right
	sbLeft := right.left // sibiling left
	h.right = sbLeft
	if sbLeft != nil {
		sbLeft.parent = h
	}

	right.left = h
	right.color = h.color
	h.color = red
	right.parent = h.parent
	h.parent = right
	return right
}

// 右旋
func rotateRight(h *lLRBTReeNode) *lLRBTReeNode {

	left := h.left
	sbRight := left.right
	h.left = sbRight
	if sbRight != nil {
		sbRight.parent = h
	}
	left.right = h
	left.color = h.color
	h.color = red
	left.parent = h.parent
	h.parent = left
	return left
}

func flipColor(h *lLRBTReeNode) {
	h.left.color = !h.left.color
	h.right.color = !h.right.color
	h.color = !h.color
}

func exchangeParent(cmp comparer.Compare, parent *lLRBTReeNode, tree *LLRBTree) {
	if parent.parent != nil {

		compare := cmp(parent.parent.key(tree.data), parent.key(tree.data))
		if compare > 0 {
			parent.parent.left = parent
		} else {
			parent.parent.right = parent
		}

	} else {
		tree.root = parent
	}
}

func (node *lLRBTReeNode) findMin() *lLRBTReeNode {

	x := node
	var min *lLRBTReeNode

	for {
		if x == nil {
			break
		}
		min = x
		x = x.left
	}

	return min

}

func (node *lLRBTReeNode) findMax() *lLRBTReeNode {

	x := node
	var max *lLRBTReeNode

	for {
		if x == nil {
			break
		}
		max = x
		x = x.right
	}

	return max

}

// Get 获取记录
func (rbTree *LLRBTree) Get(key []byte) ([]byte, error) {

	rbTree.Ref()
	defer rbTree.UnRef()

	rbTree.rw.RLock()
	defer rbTree.rw.RUnlock()
	x := rbTree.root
	for {
		if x == nil {
			break
		}
		compare := rbTree.cmp(x.key(rbTree.data), key)
		if compare > 0 {
			x = x.left
		} else if compare < 0 {
			x = x.right
		} else {
			return x.value(rbTree.data), nil
		}

	}
	return nil, ErrNotFound
}

//FindGE 获取大于等于key的最小值
func (rbTree *LLRBTree) findGE(key []byte) (*lLRBTReeNode, error) {
	x := rbTree.root
	var ge *lLRBTReeNode
	for {
		if x == nil {
			break
		}
		compare := rbTree.cmp(x.key(rbTree.data), key)
		if compare < 0 {
			x = x.right
		} else if compare > 0 {
			ge = x
			x = x.left
		} else {
			return x, nil
		}
	}

	if ge != nil {
		return ge, nil
	}
	return nil, ErrNotFound
}

//FindGE 获取大于等于key的最小值
func (rbTree *LLRBTree) FindGE(key []byte) ([]byte, error) {

	rbTree.Ref()
	defer rbTree.UnRef()

	rbTree.rw.RLock()
	defer rbTree.rw.RUnlock()

	x, err := rbTree.findGE(key)
	if err != nil {
		return nil, err
	}
	return x.key(rbTree.data), nil
}

// Cap 获取当前树的容量
func (rbTree *LLRBTree) Cap() int {
	rbTree.rw.RLock()
	defer rbTree.rw.RUnlock()
	return rbTree.capacity
}

// Len 获取当前树的size
func (rbTree *LLRBTree) Len() int {
	rbTree.rw.RLock()
	defer rbTree.rw.RUnlock()
	return rbTree.size
}

// Close 关闭rbtree
func (rbTree *LLRBTree) Close() error {
	rbTree.rw.Lock()
	if rbTree.released {
		rbTree.rw.Unlock()
		return ErrClosed
	}
	rbTree.rw.Unlock()
	rbTree.UnRef()
	return nil
}

// DebugIterString just 4 debug
func (rbTree *LLRBTree) DebugIterString() {
	rbTree.root.debugIter(rbTree.data)
}

type iterDir uint8

const (
	dirBackward iterDir = 1
	dirForward  iterDir = 2
)

// LLRBTreeIter 迭代器
type LLRBTreeIter struct {
	utils.Releaser
	released  bool
	offset    *lLRBTReeNode
	rbTree    *LLRBTree
	maxOffset *lLRBTReeNode
	minOffset *lLRBTReeNode
	soi, eoi  bool
	err       error
	iterDir
}

func NewLLRBTreeIter(rbTree *LLRBTree, releaser utils.Releaser) *LLRBTreeIter {
	maxOffset := rbTree.root.findMax()
	minOffset := rbTree.root.findMin()
	rbTree.Ref()
	return &LLRBTreeIter{
		rbTree:    rbTree,
		maxOffset: maxOffset,
		minOffset: minOffset,
		soi:       true,
		Releaser:  releaser,
	}
}

func (iter *LLRBTreeIter) Next() bool {

	iter.rbTree.rw.RLock()
	defer iter.rbTree.rw.RUnlock()

	if iter.released {
		iter.err = ErrIterReleased
		return false
	}

	if iter.eoi {
		return false
	}

	iter.iterDir = dirForward

	if iter.soi {
		iter.soi = !iter.soi
		offset := iter.minOffset
		if offset == nil {
			iter.eoi = true
			return false
		}
		iter.offset = offset
		return true
	}

	node := iter.offset

	if node == iter.maxOffset {
		iter.eoi = true
		return false
	}

	// 从它右子节点开始找
	if node.right != nil {
		iter.offset = node.right.findMin()
		return true
	}

	for {

		parent := node.parent

		if parent == nil {
			iter.eoi = true
			return false
		}

		if parent.right == node {
			node = parent
			continue
		}

		iter.offset = parent
		return true

	}

}

func (iter *LLRBTreeIter) Seek(key []byte) bool {
	iter.rbTree.rw.RLock()
	defer iter.rbTree.rw.RUnlock()

	if iter.eoi {
		return false
	}

	if iter.soi {
		iter.soi = !iter.soi
	}

	x, err := iter.rbTree.findGE(key)
	if err != nil {
		iter.err = err
		return false
	}

	iter.offset = x
	return true
}

func (iter *LLRBTreeIter) Key() []byte {
	iter.rbTree.rw.RLock()
	defer iter.rbTree.rw.RUnlock()
	if iter.soi || iter.eoi {
		return nil
	}
	return iter.offset.key(iter.rbTree.data)
}

func (iter *LLRBTreeIter) Value() []byte {
	iter.rbTree.rw.RLock()
	defer iter.rbTree.rw.RUnlock()
	if iter.soi || iter.eoi {
		return nil
	}
	return iter.offset.value(iter.rbTree.data)
}

func (iter *LLRBTreeIter) UnRef() {
	if iter.Releaser != nil {
		iter.Releaser.UnRef()
	}
	iter.rbTree.UnRef()
}

func (lLRBTReeNode *lLRBTReeNode) debugIter(data []byte) {
	if lLRBTReeNode == nil {
		return
	}
	lLRBTReeNode.left.debugIter(data)
	fmt.Printf("key=%s, value=%s\n", lLRBTReeNode.key(data), lLRBTReeNode.value(data))
	lLRBTReeNode.right.debugIter(data)
}
