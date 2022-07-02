package block_stm

import (
	"encoding/base64"
	"errors"
	"fmt"
	"sync"

	"github.com/emirpasic/gods/maps/treemap"
)

const FlagDone = 0
const FlagEstimate = 1

var (
	ErrLowerIncarnation   = errors.New("existing transaction value does not have lower incarnation")
	ErrInvalidKeyCellPath = errors.New("invalid key cell path, must already exist")
)

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

// Structure of tm (treemap):
// Key:     TxnIndex
// Value:   &WriteCell
// example: map[10:&{0 1 [53 48 101 114 58 49 49 101 114 58 53 101 114]}]
// map[TxnIndex:&{flag incarnation data}]
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

// arguments:   memory location, Version, data
// returns:     mvReadResult
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

	// TODO: could probably have a scheme where this only generally requires a read lock since any given transaction transaction
	//  should only have one incarnation executing at a time...
	cells.rw.Lock()
	defer cells.rw.Unlock()
	ci, ok := cells.tm.Get(v.TxnIndex)
	if ok {
		if ci.(*WriteCell).incarnation >= v.Incarnation {
			// ErrLowerIncarnation
			panic(fmt.Errorf("existing transaction value does not have lower incarnation: %v, %v",
				base64.StdEncoding.EncodeToString(k), v.TxnIndex))
		} else if ci.(*WriteCell).flag == FlagEstimate {
			println("marking previous estimate as done tx", v.TxnIndex, v.Incarnation)
		}
		ci.(*WriteCell).flag = FlagDone
		ci.(*WriteCell).incarnation = v.Incarnation
		ci.(*WriteCell).data = data
	} else {
		cells.tm.Put(v.TxnIndex, &WriteCell{
			flag:        FlagDone,
			incarnation: v.Incarnation,
			data:        data,
		})
	}

	return
}

func (mv *MVHashMap) MarkEstimate(k []byte, txIdx int) {

	cells := mv.getKeyCells(k, func(_ string) *TxnIndexCells {
		// ErrInvalidKeyCellPath
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

// mvReadResultDone:         read result for the current tx (depIdx != -1 , incarnation != -1)
// mvReadResultDependency:   read result for the dependency tx (depIdx != -1, incarnation = -1)
// mvReadResultNone:         nothing to read (depIdx = -1)
const (
	mvReadResultDone       = 0
	mvReadResultDependency = 1
	mvReadResultNone       = 2
)

// depIdx:        dependency Index (previous txn) at this location
// incarnation:   incarnation of previous txn at this location
// value:         value stored at this location
type mvReadResult struct {
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

// arguments:   memory location and the transaction index
// returns:     mvReadResult
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

	// fk:   depIdx
	// fv:   flag, incarnation, data (example: &{1 1 [53 48 101 114 58 49 49 101 114 58 53 101 114]})
	// Floor key is defined as the largest key that is smaller than or equal to the given key
	// this reads from the treemap, key (fk) and value (fv) of transaction with largest index than txIdx,
	// returns nil if not found
	if fk, fv := cells.tm.Floor(txIdx - 1); fk != nil && fv != nil {
		c := fv.(*WriteCell)
		switch c.flag {
		case FlagEstimate:
			res.depIdx = fk.(int)
			res.value = c.data
		case FlagDone:
			{
				res.depIdx = fk.(int)
				res.incarnation = c.incarnation
				res.value = c.data
			}
		default:
			panic(fmt.Errorf("should not happen - unknown flag value"))
		}
	}

	return
}
