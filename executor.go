package block_stm

import "fmt"

const (
	ReadKindMap     = 0
	ReadKindStorage = 1
)

type ReadDescriptor struct {
	Path []byte
	Kind int
	V    Version
}

type TxnInput []ReadDescriptor

type TxnInputOutput struct {
	inputs []TxnInput // txIdx -> input paths
}

func (io *TxnInputOutput) readSet(txnIdx int) []ReadDescriptor {
	return io.inputs[txnIdx]
}

func MakeTxnInputOutput(numTx int) *TxnInputOutput {
	return &TxnInputOutput{
		make([]TxnInput, numTx),
	}
}

func (io *TxnInputOutput) record(txId int, input []ReadDescriptor) {
	io.inputs[txId] = input
}

func validateVersion(ver Version, lastInputOutput *TxnInputOutput, versionedData *MVHashMap) (valid bool) {

	valid = true
	for _, rd := range lastInputOutput.readSet(ver.TxnIndex) {
		mvResult := versionedData.Read(rd.Path, ver.TxnIndex)
		switch mvResult.status() {
		case mvReadResultDone:
			valid = rd.Kind == ReadKindMap && rd.V == ver // TODO: this feels like checking an assertion but then treats it the same as explicit dependency?
		case mvReadResultDependency:
			valid = false
		case mvReadResultNone:
			valid = rd.Kind == ReadKindStorage // TODO: same here... feels like an assertion?
		default:
			panic(fmt.Errorf("should not happen - undefined mv read status: %v", mvResult.status()))
		}
		if !valid {
			break
		}
	}

	// TODO: set state and such depending on result of validation ???

	return
}
