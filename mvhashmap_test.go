package block_stm

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func valueFor(txIdx, inc int) []byte {
	return []byte(fmt.Sprintf("%ver:%ver:%ver", txIdx*5, txIdx+inc, inc*5))
}

func TestMVHashMapBasics(t *testing.T) {

	ap1 := []byte("/foo/b")
	ap2 := []byte("/foo/c")
	ap3 := []byte("/foo/d")

	mvh := MakeMVHashMap()

	res := mvh.Read(ap1, 5)
	require.Equal(t, -1, res.depIdx)

	mvh.Write(ap1, Version{10, 1}, valueFor(10, 1))

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
	mvh.Write(ap1, Version{8, 3}, valueFor(8, 3))

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

	// Read by txn 11 no longer observes entry from txn 10.
	res = mvh.Read(ap1, 11)
	require.Equal(t, 8, res.depIdx)
	require.Equal(t, 3, res.incarnation)
	require.Equal(t, valueFor(8, 3), res.value)

	// Reads, writes for ap2 and ap3.
	mvh.Write(ap2, Version{5, 0}, valueFor(5, 0))
	mvh.Write(ap3, Version{20, 4}, valueFor(20, 4))

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

}
