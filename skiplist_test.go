package goskiplist

import (
	"fmt"
	"testing"
	"time"
)

func TestNewSkiplist(t *testing.T) {
	skiplist := NewSkiplist()
	if skiplist == nil {
		t.Failed()
	}
}

func TestPut(t *testing.T) {
	skiplist := NewSkiplist()
	bs, err := skiplist.Put([]byte("hello"), []byte("world"))
	fmt.Println(bs, err)
}

func TestGet(t *testing.T) {
	skiplist := NewSkiplist()
	skiplist.Put([]byte("hello"), []byte("world"))
	bs, err := skiplist.Get([]byte("hello"))
	fmt.Println(string(bs), err)
}

func TestMutilPut(t *testing.T) {
	skiplist := NewSkiplist()
	skiplist.Put([]byte("hello00"), []byte("world01"))
	skiplist.Put([]byte("hello01"), []byte("world02"))
	skiplist.Put([]byte("hello02"), []byte("world03"))
	{
		bs, err := skiplist.Get([]byte("hello00"))
		fmt.Println(string(bs), err)
	}
	{
		bs, err := skiplist.Get([]byte("hello01"))
		fmt.Println(string(bs), err)
	}
	{
		bs, err := skiplist.Get([]byte("hello02"))
		fmt.Println(string(bs), err)
	}
}

func TestMutilPut1(t *testing.T) {
	skiplist := NewSkiplist()
	for i := 0; i < 100; i++ {
		v := fmt.Sprint("hello_", i)
		skiplist.Put([]byte(v), []byte("world01"))
	}

	bs, err := skiplist.Get([]byte("hello_34"))
	fmt.Println(string(bs), err)
}

func TestMutilPut2(t *testing.T) {
	skiplist := NewSkiplist()

	for i := 0; i < 1; i++ {
		go func(i int) {
			for n := 0; n < 100; n++ {
				k := fmt.Sprint("hello_", i, "_", n)
				v := fmt.Sprint("world_", i, "_", n)
				defer func() {
					err := recover()
					if err != nil {
						fmt.Println(v, err)
					}
				}()
				skiplist.Put([]byte(k), []byte(v))
			}
		}(i)
	}

	time.Sleep(1000 * 10)

	bs, err := skiplist.Get([]byte("hello_0_7"))
	fmt.Println(string(bs), err)
}
