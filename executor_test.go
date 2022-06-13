package block_stm

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestSimpleDependency(t *testing.T) {

	// assume two transactions:
	// . tx1 reads from path1 and writes to path2
	// . tx2 reads from path2 and writes to path3

	p1 := []byte("/foo/1")
	p2 := []byte("/foo/2")
	p3 := []byte("/foo/3")

	mvh := MakeMVHashMap()

	// assume these two tasks happen in parallel ...

	// ... but second tx doesn't 'see' tx1's write to p2
	res2 := mvh.Read(p2, 2)
	require.Equal(t, mvReadResultNone, res2.status())
	mvh.Write(p3, Version{2, 1}, valueFor(2, 1))

	res1 := mvh.Read(p1, 1)
	require.Equal(t, mvReadResultNone, res1.status())
	mvh.Write(p2, Version{1, 1}, valueFor(1, 1))

	lastTxIO := MakeTxnInputOutput(3) // assume there's a tx0 :)

	// recordRead read deps of tx2
	inp2 := []ReadDescriptor{{p2, ReadKindStorage, Version{2, 1}}}
	lastTxIO.recordRead(2, inp2)

	valid := validateVersion(2, lastTxIO, mvh)
	require.False(t, valid, "tx2 sees dependency on tx1 write") // would cause re-exec and re-validation of tx2

	// tx2 now 're-executes' - new incarnation
	res2 = mvh.Read(p2, 2)
	require.Equal(t, mvReadResultDone, res2.status(), "tx2 now sees 'done' write of tx1 to p2")
	mvh.Write(p3, Version{2, 2}, valueFor(2, 2))

	inp2 = []ReadDescriptor{{p2, ReadKindMap, Version{2, 2}}}
	lastTxIO.recordRead(2, inp2)

	valid = validateVersion(2, lastTxIO, mvh)
	require.True(t, valid, "tx2 is complete since dep on tx1 is satisfied")

}

type testExecTask struct {
	num  int
	wait time.Duration
}

type testIndependentExecTask struct {
	testExecTask
}

func (t testIndependentExecTask) Execute(rw BaseReadWrite) error {
	time.Sleep(t.wait)
	if _, err := rw.Read([]byte(fmt.Sprintf("test-key-%v", t.num))); err != nil {
		return err
	}
	return rw.Write([]byte(fmt.Sprintf("test-key-%v", t.num)), []byte(fmt.Sprintf("test-val-%v", t.num)))
}

type testSerialExecTask struct {
	testExecTask
}

func (t testSerialExecTask) Execute(rw BaseReadWrite) error {
	time.Sleep(t.wait)
	if _, err := rw.Read([]byte(fmt.Sprintf("test-key-%v", t.num))); err != nil {
		return err
	}
	return rw.Write([]byte(fmt.Sprintf("test-key-%v", t.num+1)), []byte(fmt.Sprintf("test-val-%v", t.num+1)))
}

type testConflictExecTask struct {
	testExecTask
}

// all tasks read and write to the same path so execution has to be 100% serial
// this simulates each task reading from and incrementing the same counter
func (t testConflictExecTask) Execute(rw BaseReadWrite) error {
	var cnt uint32
	time.Sleep(t.wait)
	if v, err := rw.Read([]byte("test-key-0")); err != nil {
		return err
	} else {
		cnt = binary.BigEndian.Uint32(v)
	}
	var b [4]byte
	cnt++
	binary.BigEndian.PutUint32(b[:], cnt)
	return rw.Write([]byte("test-key-0"), b[:])
}

var _ ExecTask = &testSerialExecTask{}
var _ ExecTask = &testConflictExecTask{}

type testBaseReadWrite struct {
}

func (t testBaseReadWrite) Read(k []byte) (v []byte, error error) {
	v = make([]byte, 4)
	binary.BigEndian.PutUint32(v, 0)
	return
}

func (t testBaseReadWrite) Write(k, v []byte) error {
	// this is just a NOP for testing purposes ...?
	return nil
}

var _ BaseReadWrite = &testBaseReadWrite{}

func validateIndependentTxOutput(txIO *TxnInputOutput) bool {
	seq := uint32(0)
	for _, v := range txIO.outputs {
		checkVal := string(v[0].Val)
		if fmt.Sprintf("test-val-%v", seq) != checkVal {
			return false
		}
		seq++
	}
	return true
}

func TestIndependentParallel(t *testing.T) {
	var exec []ExecTask
	var totalTaskDuration time.Duration
	for i := 0; i < 100; i++ {
		t := testIndependentExecTask{
			testExecTask: testExecTask{
				num:  i,
				wait: time.Duration(rand.Intn(10)+10) * time.Millisecond,
			},
		}
		exec = append(exec, t)
		totalTaskDuration += t.wait
	}
	testParallelScenario(t, exec, totalTaskDuration, validateIndependentTxOutput)
}

func validateConflictTxOutput(txIO *TxnInputOutput) bool {
	seq := uint32(1)
	for _, v := range txIO.outputs {
		if binary.BigEndian.Uint32(v[0].Val) != seq {
			return false
		}
		seq++
	}
	return true
}

func TestConflictParallel(t *testing.T) {
	var exec []ExecTask
	var totalTaskDuration time.Duration
	for i := 0; i < 100; i++ {
		t := testConflictExecTask{
			testExecTask: testExecTask{
				num:  i,
				wait: time.Duration(rand.Intn(10)+10) * time.Millisecond,
			},
		}
		exec = append(exec, t)
		totalTaskDuration += t.wait
	}
	testParallelScenario(t, exec, totalTaskDuration, validateConflictTxOutput)
}

func validateSerialTxOutput(txIO *TxnInputOutput) bool {
	seq := uint32(1)
	for _, v := range txIO.outputs {
		checkVal := string(v[0].Val)
		if fmt.Sprintf("test-val-%v", seq) != checkVal {
			return false
		}
		seq++
	}
	return true
}

func TestSerialParallel(t *testing.T) {
	var exec []ExecTask
	var totalTaskDuration time.Duration
	for i := 0; i < 100; i++ {
		t := testSerialExecTask{
			testExecTask: testExecTask{
				num:  i,
				wait: time.Duration(rand.Intn(10)+10) * time.Millisecond,
			},
		}
		exec = append(exec, t)
		totalTaskDuration += t.wait
	}
	testParallelScenario(t, exec, totalTaskDuration, validateSerialTxOutput)
}

func testParallelScenario(t *testing.T, exec []ExecTask, totalTaskDuration time.Duration, validateTxIO func(txIO *TxnInputOutput) bool) {

	var rw testBaseReadWrite

	start := time.Now()
	txIO, err := ExecuteParallel(exec, &rw)
	execDuration := time.Since(start)
	require.NoError(t, err)

	// with base parallelism and incomplete validation logic:
	// . 100-200 ms tasks: exec duration 1.604971583s, total duration 14.799s
	// . 10-20 ms tasks: exec duration 160.18275ms, total duration 1.469s
	println(fmt.Sprintf("exec duration %v, total duration %v", execDuration, totalTaskDuration))

	require.True(t, validateTxIO(txIO))
}
