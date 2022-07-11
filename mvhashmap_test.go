package block_stm

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// create data for a given txIdx and incarnation
func valueFor(txIdx, inc int) []byte {
	return []byte(fmt.Sprintf("%ver:%ver:%ver", txIdx*5, txIdx+inc, inc*5))
}

// Result of the following benchmark test:
// goos: darwin
// goarch: arm64
// pkg: github.com/paulgoleary/go-block-stm
// BenchmarkWriteTimeSameLocationDifferentTxIdx-8   	  994430	      1242 ns/op	     256 B/op	      10 allocs/op
// PASS
// ok  	github.com/paulgoleary/go-block-stm	2.636s
func BenchmarkWriteTimeSameLocationDifferentTxIdx(b *testing.B) {
	mvh2 := MakeMVHashMap()
	ap2 := []byte("/foo/b")

	randInts := []int{}
	for i := 0; i < b.N; i++ {
		randInts = append(randInts, rand.Intn(1000000000000000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mvh2.Write(ap2, Version{randInts[i], 1}, valueFor(randInts[i], 1))
	}
}

// Result of the following benchmark test:
// goos: darwin
// goarch: arm64
// pkg: github.com/paulgoleary/go-block-stm
// BenchmarkReadTimeSameLocationDifferentTxIdx-8   	 1388722	       965.2 ns/op	      24 B/op	       3 allocs/op
// PASS
// ok  	github.com/paulgoleary/go-block-stm	5.658s
func BenchmarkReadTimeSameLocationDifferentTxIdx(b *testing.B) {
	mvh2 := MakeMVHashMap()
	ap2 := []byte("/foo/b")
	txIdxSlice := []int{}
	for i := 0; i < b.N; i++ {
		txIdx := rand.Intn(1000000000000000)
		txIdxSlice = append(txIdxSlice, txIdx)
		mvh2.Write(ap2, Version{txIdx, 1}, valueFor(txIdx, 1))
	}

	b.ResetTimer()
	readRes := []mvReadResult{}
	var res mvReadResult
	for _, value := range txIdxSlice {
		res = mvh2.Read(ap2, value)
	}
	readRes = append(readRes, res)
}

// go test -run TestLowerIncarnation -v
// this will panic
// PSP - handel panic
func TestLowerIncarnation(t *testing.T) {
	ap1 := []byte("/foo/b")

	mvh := MakeMVHashMap()

	mvh.Write(ap1, Version{0, 2}, valueFor(0, 2))
	mvh.Read(ap1, 0)
	mvh.Write(ap1, Version{1, 2}, valueFor(1, 2))
	mvh.Write(ap1, Version{0, 5}, valueFor(0, 5))
	mvh.Write(ap1, Version{1, 5}, valueFor(1, 5))
	// will fail (panic) as Version{0 4} has lower incarnation than Version{0 5}
	mvh.Write(ap1, Version{0, 4}, valueFor(0, 4))
}

func TestMarkEstimate(t *testing.T) {
	ap1 := []byte("/foo/b")

	mvh := MakeMVHashMap()

	mvh.Write(ap1, Version{7, 2}, valueFor(7, 2))
	mvh.MarkEstimate(ap1, 7)
	mvh.Write(ap1, Version{7, 4}, valueFor(7, 4))
}

func TestTimeComplexity(t *testing.T) {

	// for 1000000 read and write with no dependency at different memory location
	// takes around 1.1 - 1.2 seconds
	// only Write: 1 second
	// only Read: 0.23 seconds (as it is not reaching the Floor function)
	mvh1 := MakeMVHashMap()
	for i := 0; i < 1000000; i++ {
		ap1 := []byte(fmt.Sprint(i))
		mvh1.Write(ap1, Version{i, 1}, valueFor(i, 1))
		mvh1.Read(ap1, i)
	}
	// fmt.Println("\nMVHashMap:", "\n ", mvh1)

	// for 1000000 read and write with dependency at same memory location
	// takes around 1.1 - 1.2 seconds
	// only Write: 0.8 seconds
	// only Read: 0.1 - 0.2 seconds (as it is not reaching the Floor function)
	mvh2 := MakeMVHashMap()
	ap2 := []byte("/foo/b")
	for i := 0; i < 1000000; i++ {
		mvh2.Write(ap2, Version{i, 1}, valueFor(i, 1))
		mvh2.Read(ap2, i)
	}
	// fmt.Println("\nMVHashMap:", "\n ", mvh2)

}

// around 0.85 seconds
func TestWriteTimeSameLocationDifferentTxnIdx(t *testing.T) {
	mvh1 := MakeMVHashMap()
	ap1 := []byte("/foo/b")
	for i := 0; i < 1000000; i++ {
		mvh1.Write(ap1, Version{i, 1}, valueFor(i, 1))
	}
	// fmt.Println("\nMVHashMap:", "\n ", mvh2)
}

// around 0.35 seconds
func TestWriteTimeSameLocationSameTxnIdx(t *testing.T) {
	mvh1 := MakeMVHashMap()
	ap1 := []byte("/foo/b")
	for i := 0; i < 1000000; i++ {
		mvh1.Write(ap1, Version{1, i}, valueFor(i, 1))
	}
	// fmt.Println("\nMVHashMap:", "\n ", mvh2)
}

// around 1.0 seconds
func TestWriteTimeDifferentLocation(t *testing.T) {
	mvh1 := MakeMVHashMap()
	for i := 0; i < 1000000; i++ {
		ap1 := []byte(fmt.Sprint(i))
		mvh1.Write(ap1, Version{i, 1}, valueFor(i, 1))
	}
	// fmt.Println("\nMVHashMap:", "\n ", mvh2)
}

// around 0.18 seconds
func TestReadTimeSameLocation(t *testing.T) {
	mvh1 := MakeMVHashMap()
	ap1 := []byte("/foo/b")
	mvh1.Write(ap1, Version{1, 1}, valueFor(1, 1))
	for i := 0; i < 1000000; i++ {
		mvh1.Read(ap1, 2)
	}
	// fmt.Println("\nMVHashMap:", "\n ", mvh1)
}

// to view logs, run test with:
// go test -run TestMVHashMapBasics -v
func TestMVHashMapBasics(t *testing.T) {
	fmt.Println("\nIn TestMVHashMapBasics")

	// t.Logf("Hello")
	fmt.Println("Hello Again!")

	// memory locations
	ap1 := []byte("/foo/b")
	ap2 := []byte("/foo/c")
	ap3 := []byte("/foo/d")

	mvh := MakeMVHashMap()
	fmt.Println("\nmvh:", mvh)

	res := mvh.Read(ap1, 5)
	require.Equal(t, -1, res.depIdx)

	mvh.Write(ap1, Version{10, 1}, valueFor(10, 1))
	fmt.Println("\nmvh:", mvh)

	res = mvh.Read(ap1, 9)
	require.Equal(t, -1, res.depIdx, "reads that should go the the DB return dependency -1")
	res = mvh.Read(ap1, 10)
	require.Equal(t, -1, res.depIdx, "Read returns entries from smaller txns, not txn 10")

	// Reads for a higher txn return the entry written by txn 10.
	res = mvh.Read(ap1, 15)
	require.Equal(t, 10, res.depIdx, "reads for a higher txn return the entry written by txn 10.")
	require.Equal(t, 1, res.incarnation)
	require.Equal(t, valueFor(10, 1), res.value)

	// More writes.
	mvh.Write(ap1, Version{12, 0}, valueFor(12, 0))
	fmt.Println("\nmvh:", mvh)
	mvh.Write(ap1, Version{8, 3}, valueFor(8, 3))
	fmt.Println("\nmvh:", mvh)

	// Verify reads.
	res = mvh.Read(ap1, 15)
	require.Equal(t, 12, res.depIdx)
	require.Equal(t, 0, res.incarnation)
	require.Equal(t, valueFor(12, 0), res.value)

	res = mvh.Read(ap1, 11)
	require.Equal(t, 10, res.depIdx)
	require.Equal(t, 1, res.incarnation)
	require.Equal(t, valueFor(10, 1), res.value)

	res = mvh.Read(ap1, 10)
	require.Equal(t, 8, res.depIdx)
	require.Equal(t, 3, res.incarnation)
	require.Equal(t, valueFor(8, 3), res.value)

	// Mark the entry written by 10 as an estimate.
	mvh.MarkEstimate(ap1, 10)

	res = mvh.Read(ap1, 11)
	require.Equal(t, 10, res.depIdx)
	require.Equal(t, -1, res.incarnation, "dep at tx 10 is now an estimate")

	// Delete the entry written by 10, write to a different ap.
	mvh.Delete(ap1, 10)
	mvh.Write(ap2, Version{10, 2}, valueFor(10, 2))
	fmt.Println("\nmvh:", mvh)

	// Read by txn 11 no longer observes entry from txn 10.
	res = mvh.Read(ap1, 11)
	require.Equal(t, 8, res.depIdx)
	require.Equal(t, 3, res.incarnation)
	require.Equal(t, valueFor(8, 3), res.value)

	// Reads, writes for ap2 and ap3.
	mvh.Write(ap2, Version{5, 0}, valueFor(5, 0))
	fmt.Println("\nmvh:", mvh)
	mvh.Write(ap3, Version{20, 4}, valueFor(20, 4))
	fmt.Println("\nmvh:", mvh)

	res = mvh.Read(ap2, 10)
	require.Equal(t, 5, res.depIdx)
	require.Equal(t, 0, res.incarnation)
	require.Equal(t, valueFor(5, 0), res.value)

	res = mvh.Read(ap3, 21)
	require.Equal(t, 20, res.depIdx)
	require.Equal(t, 4, res.incarnation)
	require.Equal(t, valueFor(20, 4), res.value)

	// Clear ap1 and ap3.
	mvh.Delete(ap1, 12)
	mvh.Delete(ap1, 8)
	mvh.Delete(ap3, 20)

	// Reads from ap1 and ap3 go to db.
	res = mvh.Read(ap1, 30)
	require.Equal(t, -1, res.depIdx)

	res = mvh.Read(ap3, 30)
	require.Equal(t, -1, res.depIdx)

	// No-op delete at ap2 - doesn't panic because ap2 does exist
	mvh.Delete(ap2, 11)

	// Read entry by txn 10 at ap2.
	res = mvh.Read(ap2, 15)
	require.Equal(t, 10, res.depIdx)
	require.Equal(t, 2, res.incarnation)
	require.Equal(t, valueFor(10, 2), res.value)

	fmt.Println("\nmvh:", mvh)
}
