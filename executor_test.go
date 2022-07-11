package block_stm

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestCoreDependency(t *testing.T) {

	// assume two transactions:
	// . tx0 reads from path0 and writes to path1
	// . tx1 reads from path1 and writes to path2

	p0 := []byte("/foo/0")
	p1 := []byte("/foo/1")
	p2 := []byte("/foo/2")

	mvh := MakeMVHashMap()
	lastTxIO := MakeTxnInputOutput(2)

	// assume these two tasks happen in parallel ...

	// ... but second tx doesn't 'see' tx0's write to p1
	res1 := mvh.Read(p1, 1)
	require.Equal(t, mvReadResultNone, res1.status(), "tx1 read from disk")
	mvh.Write(p2, Version{1, 0}, valueFor(1, 0))
	// recordRead read dep of tx1
	inp1 := []ReadDescriptor{res1.rd(p1)}
	lastTxIO.recordRead(1, inp1)

	res0 := mvh.Read(p0, 0)
	require.Equal(t, mvReadResultNone, res0.status(), "tx0 read from disk")
	mvh.Write(p1, Version{0, 0}, valueFor(0, 0))
	// recordRead read dep of tx0
	inp0 := []ReadDescriptor{res0.rd(p0)}
	lastTxIO.recordRead(0, inp0)

	valid := validateVersion(1, lastTxIO, mvh)
	require.False(t, valid, "tx1 sees dependency on tx0 write") // would cause re-exec and re-validation of tx1

	// tx1 now 're-executes' - new incarnation
	res1 = mvh.Read(p1, 1)
	require.Equal(t, mvReadResultDone, res1.status(), "tx1 now sees 'done' write of tx0 to p1")
	mvh.Write(p2, Version{1, 1}, valueFor(1, 1))
	// recordRead read dep of tx1
	inp1 = []ReadDescriptor{res1.rd(p1)}
	lastTxIO.recordRead(1, inp1)

	valid = validateVersion(1, lastTxIO, mvh)
	require.True(t, valid, "tx1 is complete since dep on tx0 is satisfied")
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
