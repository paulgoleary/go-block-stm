package block_stm

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSimpleDependency(t *testing.T) {

	// assume two transactions:
	// . tx 1 reads from path1 and writes to path2
	// . tx2 reads from path2 and writes to path3

	p1 := []byte("/foo/1")
	p2 := []byte("/foo/2")
	p3 := []byte("/foo/3")

	mvh := MakeMVHashMap()

	// assume these two tasks happen in parallel ...

	// ... but second tx doesn't 'see' tx1's write to p2
	res := mvh.Read(p2, 2)
	require.Equal(t, mvReadResultNone, res.status())
	mvh.Write(p3, Version{2, 1}, valueFor(2, 1))

	res = mvh.Read(p1, 1)
	require.Equal(t, mvReadResultNone, res.status())
	mvh.Write(p2, Version{1, 1}, valueFor(1, 1))

	lastTxIO := MakeTxnInputOutput(3) // assume there's a tx0 :)

	// record read deps of tx2
	inp2 := []ReadDescriptor{{p2, ReadKindStorage, Version{2, 1}}}
	lastTxIO.record(2, inp2)

	valid := validateVersion(Version{2, 1}, lastTxIO, mvh)
	require.False(t, valid, "tx2 sees dependency on tx1 write") // would cause re-exec and re-validation of tx2

	// tx2 now 're-executes' - new incarnation
	res = mvh.Read(p2, 2)
	require.Equal(t, mvReadResultDone, res.status(), "tx2 now sees 'done' write of tx1 to p2")
	mvh.Write(p3, Version{2, 2}, valueFor(2, 2))

	inp2 = []ReadDescriptor{{p2, ReadKindMap, Version{2, 2}}}
	lastTxIO.record(2, inp2)

	valid = validateVersion(Version{2, 2}, lastTxIO, mvh)
	require.True(t, valid, "tx2 is complete since dep on tx1 is satisfied")

}
