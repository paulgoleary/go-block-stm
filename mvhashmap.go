package block_stm

import (
	"encoding/base64"
	"fmt"
	"github.com/emirpasic/gods/maps/treemap"
	"sync"
)

const FlagDone = 0
const FlagEstimate = 1

type MVHashMap struct {
	rw sync.RWMutex
	m  map[string]*TxnIndexCells // TODO: might want a more efficient key representation
}

func MakeMVHashMap() *MVHashMap {
	return &MVHashMap{
		rw: sync.RWMutex{},
		m:  make(map[string]*TxnIndexCells),
	}
}

type WriteCell struct {
	flag        uint
	incarnation int
	data        []byte
}

type TxnIndexCells struct {
	rw sync.RWMutex
	tm *treemap.Map
}

type Version struct {
	TxnIndex    int
	Incarnation int
}

func (mv *MVHashMap) getKeyCells(k []byte, fNoKey func(kenc string) *TxnIndexCells) (cells *TxnIndexCells) {
	kenc := base64.StdEncoding.EncodeToString(k)
	var ok bool
	mv.rw.RLock()
	cells, ok = mv.m[kenc]
	mv.rw.RUnlock()
	if !ok {
		cells = fNoKey(kenc)
	}
	return
}

func (mv *MVHashMap) Write(k []byte, v Version, data []byte) {

	cells := mv.getKeyCells(k, func(kenc string) (cells *TxnIndexCells) {
		n := &TxnIndexCells{
			rw: sync.RWMutex{},
			tm: treemap.NewWithIntComparator(),
		}
		var ok bool
		mv.rw.Lock()
		if cells, ok = mv.m[kenc]; !ok {
			mv.m[kenc] = n
			cells = n
		}
		mv.rw.Unlock()
		return
	})

	cells.rw.RLock()
	ci, ok := cells.tm.Get(v.TxnIndex)
	cells.rw.RUnlock()
	if ok {
		if ci.(*WriteCell).incarnation >= v.Incarnation {
			panic(fmt.Errorf("existing transaction value does not have lower incarnation: %v, %v",
				base64.StdEncoding.EncodeToString(k), v.TxnIndex))
		} else if ci.(*WriteCell).flag == FlagEstimate {
			println("marking previous estimate as done tx", v.TxnIndex, v.Incarnation)
		}
		// TODO: this may not be totally safe but trying it for now !!!
		ci.(*WriteCell).flag = FlagDone
		ci.(*WriteCell).incarnation = v.Incarnation
		ci.(*WriteCell).data = data
	} else {
		cells.rw.Lock()
		cells.tm.Put(v.TxnIndex, &WriteCell{
			flag:        FlagDone,
			incarnation: v.Incarnation,
			data:        data,
		})
		cells.rw.Unlock()
	}

	return
}

func (mv *MVHashMap) MarkEstimate(k []byte, txIdx int) {

	cells := mv.getKeyCells(k, func(_ string) *TxnIndexCells {
		panic(fmt.Errorf("path must already exist"))
	})

	cells.rw.RLock()
	if ci, ok := cells.tm.Get(txIdx); !ok {
		panic("should not happen - cell should be present for path")
	} else {
		ci.(*WriteCell).flag = FlagEstimate
	}
	cells.rw.RUnlock()
}

func (mv *MVHashMap) Delete(k []byte, txIdx int) {
	cells := mv.getKeyCells(k, func(_ string) *TxnIndexCells {
		panic(fmt.Errorf("path must already exist"))
	})

	cells.rw.Lock()
	defer cells.rw.Unlock()
	cells.tm.Remove(txIdx)
}

const (
	mvReadResultDone       = 0
	mvReadResultDependency = 1
	mvReadResultNone       = 2
)

type mvReadResult struct {
	ver         Version
	depIdx      int
	incarnation int
	value       []byte
}

func (mvr mvReadResult) status() int {
	if mvr.depIdx != -1 {
		if mvr.incarnation == -1 {
			return mvReadResultDependency
		} else {
			return mvReadResultDone
		}
	}
	return mvReadResultNone
}

func (mv *MVHashMap) Read(k []byte, txIdx int) (res mvReadResult) {

	res.depIdx = -1
	res.incarnation = -1

	cells := mv.getKeyCells(k, func(_ string) *TxnIndexCells {
		return nil
	})
	if cells == nil {
		return
	}

	cells.rw.RLock()
	defer cells.rw.RUnlock()

	iter := cells.tm.Iterator()
	iter.End()
	if iter.PrevTo(func(k interface{}, v interface{}) bool {
		if k.(int) < txIdx {
			return true
		}
		return false
	}) {
		c := iter.Value().(*WriteCell)
		switch c.flag {
		case FlagEstimate:
			res.depIdx = iter.Key().(int)
			res.value = c.data
		case FlagDone:
			{
				res.depIdx = iter.Key().(int)
				res.incarnation = c.incarnation
				res.value = c.data
			}
		default:
			panic(fmt.Errorf("should not happen - unknown flag value"))
		}
	}
	return
}
