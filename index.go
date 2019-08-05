package goskiplist

import (
	"sync/atomic"
	"unsafe"
)

type Node struct {
	key   *[]byte
	value *interface{}
	next  *Node
}

func NewNode(key *[]byte, value *interface{}, next *Node) *Node {
	p := new(Node)
	p.key = key
	p.value = value
	p.next = next
	return p
}

func (this *Node) casValue(cmp *interface{}, val *interface{}) bool {
	valueAddr := (*uintptr)(unsafe.Pointer(&this.value))
	cmp_uintptr := uintptr(unsafe.Pointer(cmp))
	val_uintptr := uintptr(unsafe.Pointer(val))
	return atomic.CompareAndSwapUintptr(valueAddr, cmp_uintptr, val_uintptr)
}

func (this *Node) casNext(cmp *Node, val *Node) bool {
	nextAddr := (*uintptr)(unsafe.Pointer(&this.next))
	cmp_uintptr := uintptr(unsafe.Pointer(cmp))
	val_uintptr := uintptr(unsafe.Pointer(val))
	res := atomic.CompareAndSwapUintptr(nextAddr, cmp_uintptr, val_uintptr)
	return res
}

func (this *Node) helpDelete(b *Node, f *Node) {
	if f == this.next && this == b.next {
		if f == nil || unsafe.Pointer(f.value) != unsafe.Pointer(f) { // not already marked
			pNewNode := NewNode(nil, (*interface{})(unsafe.Pointer(uintptr(unsafe.Pointer(this)))), f)
			this.casNext(f, pNewNode)
		} else {
			b.casNext(this, f.next)
		}
	}
}

type Index struct {
	node  *Node
	down  *Index
	right *Index
}

func NewIndex(node *Node, down *Index, right *Index) *Index {
	return &Index{
		node:  node,
		down:  down,
		right: right,
	}
}

type HeadIndex struct {
	Index
	level int
}

func (this *Index) unlink(succ *Index) bool {
	return this.node.value != nil && this.casRight(succ, succ.right)
}

func (this *Index) casRight(cmp *Index, val *Index) bool {
	rightAddr := (*uintptr)(unsafe.Pointer(&this.right))
	cmp_uintptr := uintptr(unsafe.Pointer(cmp))
	val_uintptr := uintptr(unsafe.Pointer(val))
	return atomic.CompareAndSwapUintptr(rightAddr, cmp_uintptr, val_uintptr)
}

func (this *Index) link(succ *Index, newSucc *Index) bool {
	n := this.node
	newSucc.right = succ
	res := n.value != nil && this.casRight(succ, newSucc)
	return res
}

func NewHeadIndex(node *Node, down *Index, right *Index, level int) *HeadIndex {
	p := new(HeadIndex)
	//p.Index = new(Index)
	p.node = node
	p.down = down
	p.right = right
	p.level = level
	return p
}
