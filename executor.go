package block_stm

import (
	"encoding/base64"
	"fmt"
)

func validateVersion(txIdx int, lastInputOutput *TxnInputOutput, versionedData *MVHashMap) (valid bool) {

	valid = true
	for _, rd := range lastInputOutput.readSet(txIdx) {
		mvResult := versionedData.Read(rd.Path, txIdx)
		switch mvResult.status() {
		case mvReadResultDone:
			valid = rd.Kind == ReadKindMap
			valid = valid && rd.V == Version{
				TxnIndex:    mvResult.depIdx,
				Incarnation: mvResult.incarnation,
			}
		case mvReadResultDependency:
			valid = false
		case mvReadResultNone:
			valid = rd.Kind == ReadKindStorage // feels like an assertion?
		default:
			panic(fmt.Errorf("should not happen - undefined mv read status: %ver", mvResult.status()))
		}
		if !valid {
			break
		}
	}

	return
}

type ExecResult struct {
	err   error
	ver   Version
	txIn  TxnInput
	txOut TxnOutput
}

type ExecTask interface {
	Execute(rw BaseReadWrite) error
}

type BaseReadWrite interface {
	Read(k []byte) (v []byte, error error)
	Write(k, v []byte) error
}

type ExecVersionView struct {
	ver Version
	et  ExecTask
	rw  BaseReadWrite
	mvh *MVHashMap

	readMap  map[string]ReadDescriptor
	writeMap map[string]WriteDescriptor
}

func (ev *ExecVersionView) ensureReadMap() {
	if ev.readMap == nil {
		ev.readMap = make(map[string]ReadDescriptor)
	}
}

func (ev *ExecVersionView) ensureWriteMap() {
	if ev.writeMap == nil {
		ev.writeMap = make(map[string]WriteDescriptor)
	}
}

func (ev *ExecVersionView) Execute() (er ExecResult) {
	er.ver = ev.ver
	if er.err = ev.et.Execute(ev); er.err != nil {
		println(fmt.Sprintf("executed task - failed %v.%v, err %v", ev.ver.TxnIndex, ev.ver.Incarnation, er.err))
		return
	}
	for _, v := range ev.readMap {
		er.txIn = append(er.txIn, v)
	}
	for _, v := range ev.writeMap {
		er.txOut = append(er.txOut, v)
	}
	println(fmt.Sprintf("executed task %v.%v, in %v, out %v", ev.ver.TxnIndex, ev.ver.Incarnation,
		len(er.txIn), len(er.txOut)))
	return
}

var errExecAbort = fmt.Errorf("execution aborted with dependency")

func (ev *ExecVersionView) Read(k []byte) (v []byte, err error) {
	ev.ensureReadMap()
	res := ev.mvh.Read(k, ev.ver.TxnIndex)
	switch res.status() {
	case mvReadResultDone:
		{
			v = res.value
		}
	case mvReadResultDependency:
		{
			return nil, errExecAbort
		}
	case mvReadResultNone:
		{
			v, err = ev.rw.Read(k)
		}
	default:
		return nil, fmt.Errorf("should not happen - invalid read result status '%ver'", res.status())
	}
	rd := res.rd(k)
	mk := base64.StdEncoding.EncodeToString(k)
	// TODO: I assume we don't want to overwrite an existing read because this could - for example - change a storage
	//  read to map if the same value is read multiple times.
	if _, ok := ev.readMap[mk]; !ok {
		ev.readMap[mk] = rd
	}
	return
}

func (ev *ExecVersionView) Write(k, v []byte) error {
	ev.ensureWriteMap()
	ev.mvh.Write(k, ev.ver, v)
	mk := base64.StdEncoding.EncodeToString(k)
	ev.writeMap[mk] = WriteDescriptor{
		Path: k,
		V:    ev.ver,
		Val:  v,
	}
	return nil
}

const numGoProcs = 10

func ExecuteParallel(tasks []ExecTask, rw BaseReadWrite) (lastTxIO *TxnInputOutput, err error) {

	chTasks := make(chan ExecVersionView, len(tasks))
	chResults := make(chan ExecResult, len(tasks))
	chDone := make(chan bool)

	var cntExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail int

	for i := 0; i < numGoProcs; i++ {
		go func(procNum int, t chan ExecVersionView) {
		Loop:
			for {
				select {
				case task := <-t:
					{
						res := task.Execute()
						chResults <- res
					}
				case <-chDone:
					break Loop
				}
			}
			println(fmt.Sprintf("proc done %v", procNum)) // TODO: logging ...
		}(i, chTasks)
	}

	mvh := MakeMVHashMap()

	execTasks := makeStatusManager(len(tasks))
	validateTasks := makeStatusManager(0)

	// bootstrap execution
	for x := 0; x < numGoProcs; x++ {
		tx := execTasks.takeNextPending()
		if tx != -1 {
			cntExec++
			chTasks <- ExecVersionView{ver: Version{tx, 0}, et: tasks[tx], rw: rw, mvh: mvh}
		}
	}

	lastTxIO = MakeTxnInputOutput(len(tasks))
	txIncarnations := make([]int, len(tasks))

	diagExecSuccess := make([]int, len(tasks))
	diagExecAbort := make([]int, len(tasks))

	for {
		res := <-chResults
		switch res.err {
		case nil:
			{
				lastTxIO.recordRead(res.ver.TxnIndex, res.txIn)
				if res.ver.Incarnation == 0 {
					lastTxIO.recordWrite(res.ver.TxnIndex, res.txOut)
				} else {
					if res.txOut.hasNewWrite(lastTxIO.writeSet(res.ver.TxnIndex)) {
						// TODO: 'if there is a write to a memory location ...'
						// panic("test + implement me")
						println("TODO: deal with different write sets")
					}
					lastTxIO.recordWrite(res.ver.TxnIndex, res.txOut)
				}
				validateTasks.pushPending(res.ver.TxnIndex)
				execTasks.markComplete(res.ver.TxnIndex)
				if diagExecSuccess[res.ver.TxnIndex] > 0 && diagExecAbort[res.ver.TxnIndex] == 0 {
					println("got multiple successful execution w/o abort?", res.ver.TxnIndex, res.ver.Incarnation)
				}
				diagExecSuccess[res.ver.TxnIndex]++
				cntSuccess++
			}
		case errExecAbort:
			{
				// bit of a subtle / tricky bug here. this adds the tx back to pending ...
				execTasks.revertInProgress(res.ver.TxnIndex)
				// ... but the incarnation needs to be bumped
				txIncarnations[res.ver.TxnIndex]++
				diagExecAbort[res.ver.TxnIndex]++
				cntAbort++
			}
		default:
			{
				err = res.err
				break
			}
		}

		// if we got more work, queue one up...
		nextTx := execTasks.takeNextPending()
		if nextTx != -1 {
			cntExec++
			chTasks <- ExecVersionView{ver: Version{nextTx, txIncarnations[nextTx]}, et: tasks[nextTx], rw: rw, mvh: mvh}
		}

		// do validations ...
		maxComplete := execTasks.maxAllComplete()

		const validationIncrement = 5
		cntValidate := validateTasks.countPending()
		// if we're currently done with all execution tasks then let's validate everything; otherwise do one increment ...
		if execTasks.countComplete() != len(tasks) && cntValidate > validationIncrement {
			cntValidate = validationIncrement
		}
		var toValidate []int
		for i := 0; i < cntValidate; i++ {
			if validateTasks.minPending() <= maxComplete {
				toValidate = append(toValidate, validateTasks.takeNextPending())
			} else {
				break
			}
		}

		for i := 0; i < len(toValidate); i++ {
			cntTotalValidations++
			tx := toValidate[i]
			if validateVersion(tx, lastTxIO, mvh) {
				println(fmt.Sprintf("* completed validation task %v", tx))
				validateTasks.markComplete(tx)
			} else {
				println(fmt.Sprintf("* validation task FAILED %v", tx))
				cntValidationFail++
				diagExecAbort[tx]++
				for _, v := range lastTxIO.writeSet(tx) {
					mvh.MarkEstimate(v.Path, tx)
				}
				// 'create validation tasks for all transactions > tx ...'
				validateTasks.pushPendingSet(execTasks.getRevalidationRange(tx + 1))
				validateTasks.clearInProgress(tx) // clear in progress - pending will be added again once new incarnation executes
				if execTasks.checkPending(tx) {
					// println() // have to think about this ...
				} else {
					execTasks.pushPending(tx)
					execTasks.clearComplete(tx)
					txIncarnations[tx]++
				}
			}
		}

		// if we didn't queue work previously, do check again so we keep making progress ...
		if nextTx == -1 {
			nextTx = execTasks.takeNextPending()
			if nextTx != -1 {
				cntExec++
				chTasks <- ExecVersionView{ver: Version{nextTx, txIncarnations[nextTx]}, et: tasks[nextTx], rw: rw, mvh: mvh}
			}
		}

		if validateTasks.countComplete() == len(tasks) && execTasks.countComplete() == len(tasks) {
			println(fmt.Sprintf("exec summary: %v execs: %v success, %v aborts; %v validations: %v failures",
				cntExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail))
			break
		}
	}

	for i := 0; i < numGoProcs; i++ {
		chDone <- true
	}
	close(chTasks)
	close(chResults)

	return
}
