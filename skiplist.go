package goskiplist

import (
	"bytes"
	"errors"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

type SkipList struct {
	head *HeadIndex
}

var cmp = func(k1 []byte, k2 []byte) int {
	return bytes.Compare(k1, k2)
}

var BASE_HEADER = make([]byte, 0)

func NewSkiplist() *SkipList {
	p := (*interface{})(unsafe.Pointer(uintptr(unsafe.Pointer(&BASE_HEADER))))
	node := NewNode(nil, p, nil)
	return &SkipList{
		head: NewHeadIndex(node, nil, nil, 1),
	}
}

func (this *SkipList) Put(key []byte, value []byte) ([]byte, error) {
	if value == nil {
		return nil, errors.New("NullPointerException")
	}

	pv, err := this.doPut(key, value, false)
	if err != nil {
		return nil, err
	}

	if pv == nil {
		return nil, nil
	}
	pvbytes := (*[]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(pv))))
	return *pvbytes, err
}

func (this *SkipList) doPut(key []byte, value []byte, onlyIfAbsent bool) (*interface{}, error) {
	var z *Node
	if key == nil {
		return nil, errors.New("NullPointerException")
	}
outer:
	for {
		b, err := this.findPredecessor(key, cmp)
		if err != nil {
			return nil, err
		}
		n := b.next

		for {
			if n != nil {
				//Object v; int c;
				f := n.next
				if n != b.next { // inconsistent read
					break
				}

				v := n.value
				if v == nil { // n is deleted
					n.helpDelete(b, f)
					break
				}

				if b.value == nil || unsafe.Pointer(v) == unsafe.Pointer(n) { // b is deleted
					break
				}

				c := cmp(key, *n.key)
				if c > 0 {
					b = n
					n = f
					continue
				}

				if c == 0 {
					pvalue := (*interface{})(unsafe.Pointer(uintptr(unsafe.Pointer(&value))))
					if onlyIfAbsent || n.casValue(v, pvalue) {
						return v, nil
					}

					break // restart if lost race to replace value
				}
				// else c < 0; fall through
			}

			pvalue := (*interface{})(unsafe.Pointer(uintptr(unsafe.Pointer(&value))))
			z = NewNode(&key, pvalue, n)
			if !b.casNext(n, z) {
				break // restart if lost race to append to b
			}
			break outer
		}
	}

	rnd := rand.Int31()
	x := 0x80000001
	if (rnd & int32(x)) == 0 { // test highest and lowest bits
		var level int = 1
		var max int

		for {
			rnd >>= 1
			if rnd&1 != 0 {
				level++
			} else {
				break
			}
		}

		var idx *Index = nil
		h := this.head
		max = h.level
		if level <= max {
			for i := 1; i <= level; i++ {
				idx = NewIndex(z, idx, nil)
			}
		} else { // try to grow by one level
			level = max + 1 // hold in array and later pick the one to use
			idxs := make([]*Index, level+1)
			for i := 1; i <= level; i++ {
				idx = NewIndex(z, idx, nil)
				idxs[i] = idx
			}
			for {
				h = this.head
				oldLevel := h.level
				if level <= oldLevel { // lost race to add level
					break
				}
				var newh *HeadIndex = h
				oldbase := h.node
				for j := oldLevel + 1; j <= level; j++ {
					pindex := (*Index)(unsafe.Pointer(uintptr((unsafe.Pointer(newh)))))
					newh = NewHeadIndex(oldbase, pindex, idxs[j], j)
				}
				if this.casHead(h, newh) {
					h = newh
					level = oldLevel
					idx = idxs[level]
					break
				}
			}
		}
		// find insertion points and splice in

	splice:
		for insertionLevel := level; ; {
			var j = h.level
			var q *Index = &h.Index
			var r = q.right
			var t = idx
			for {
				if q == nil || t == nil {
					break splice
				}
				if r != nil {
					n := r.node
					// compare before deletion check avoids needing recheck
					c := cmp(key, *n.key)
					if n.value == nil {
						if !q.unlink(r) {
							break
						}

						r = q.right
						continue
					}

					if c > 0 {
						q = r
						r = r.right
						continue
					}
				}

				if j == insertionLevel {
					if !q.link(r, t) {
						break // restart
					}

					if t.node.value == nil {
						this.findNode(key)
						break splice
					}

					insertionLevel--
					if insertionLevel == 0 {
						break splice
					}
				}

				j--
				if j >= insertionLevel && j < level {
					t = t.down
				}

				q = q.down
				r = q.right
			}
		}
	}

	return nil, nil
}

func (this *SkipList) findPredecessor(key []byte, cmp func([]byte, []byte) int) (*Node, error) {
	if key == nil {
		return nil, errors.New("NullPointerException")
	}

	for {
		var q *Index = &(this.head.Index)
		r := q.right
		var d *Index
		for {
			if r != nil {
				n := r.node
				k := n.key
				if n.value == nil {
					if !q.unlink(r) {
						break // restart
					}
					r = q.right // reread r
					continue
				}

				if cmp(key, *k) > 0 {
					q = r
					r = r.right
					continue
				}
			}

			d = q.down
			if d == nil {
				return q.node, nil
			}

			q = d
			r = d.right
		}
	}
}

func (this *SkipList) findNode(key []byte) (*Node, error) {
	if key == nil {
		return nil, errors.New("NullPointerException") // don't postpone errors
	}

outer:
	for {
		b, err := this.findPredecessor(key, cmp)
		if err != nil {
			return nil, err
		}

		n := b.next
		for {
			//Object v; int c;
			if n == nil {
				break outer
			}

			f := n.next
			if n != b.next { // inconsistent read
				break
			}

			v := n.value
			if v == nil { // n is deleted
				n.helpDelete(b, f)
				break
			}

			if b.value == nil || unsafe.Pointer(v) == unsafe.Pointer(n) { // b is deleted
				break
			}

			c := cmp(key, *n.key)
			if c == 0 {
				return n, nil
			}

			if c < 0 {
				break outer
			}

			b = n
			n = f
		}
	}

	return nil, nil
}

func (this *SkipList) casHead(cmp *HeadIndex, val *HeadIndex) bool {
	headAddr := (*uintptr)(unsafe.Pointer(&this.head))
	cmp_uintptr := uintptr(unsafe.Pointer(cmp))
	val_uintptr := uintptr(unsafe.Pointer(val))
	return atomic.CompareAndSwapUintptr(headAddr, cmp_uintptr, val_uintptr)
}

func (this *SkipList) Get(key []byte) ([]byte, error) {
	return this.doGet(key)
}

func (this *SkipList) doGet(key []byte) ([]byte, error) {
	node, err := this.findNode(key)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, nil
	}
	p := (*[]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(node.value))))
	return *p, nil
}
