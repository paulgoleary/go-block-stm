package state

import (
	"fmt"
	estate "github.com/0xPolygon/eth-state-transition"
	itrie "github.com/0xPolygon/eth-state-transition/immutable-trie"
	"github.com/0xPolygon/eth-state-transition/runtime"
	"github.com/0xPolygon/eth-state-transition/types"
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
)

var testGasPrice = big.NewInt(10_000)
var testFromAddr = types.BytesToAddress([]byte{0xde, 0xad, 0xbe, 0xef})
var testToAddr = types.BytesToAddress([]byte{0xca, 0xfe, 0xba, 0xbe})

func makeTestTransaction() *estate.Transaction {
	return &estate.Transaction{
		Nonce:    0,
		GasPrice: testGasPrice,
		Gas:      100_000,
		To:       &testToAddr,
		Value:    big.NewInt(100_000),
		Input:    nil,
		Hash:     types.Hash{},
		From:     testFromAddr,
	}
}

func makeTestInitialState() estate.SnapshotWriter {
	storage := itrie.NewMemoryStorage()
	archiveState := itrie.NewArchiveState(storage)
	snap := archiveState.NewSnapshot()

	testObj := estate.Object{
		Address:   testFromAddr,
		CodeHash:  types.Hash{},
		Balance:   big.NewInt(0),
		Root:      types.Hash{},
		Nonce:     0,
		Deleted:   false,
		DirtyCode: false,
		Code:      nil,
		Storage:   nil,
	}
	testObj.Balance, _ = testObj.Balance.SetString("1000000000000000000", 10)
	newSnap, _ := snap.Commit([]*estate.Object{&testObj})
	return newSnap
}

func TestStateBasics(t *testing.T) {

	snap := makeTestInitialState()

	// create a transition object
	forks := runtime.ForksInTime{}
	config := runtime.TxContext{}
	config.GasLimit = 50_000_000
	transition := estate.NewTransition(forks, config, snap)

	tx := makeTestTransaction()
	require.True(t, transition.AccountExists(tx.From))

	// process a transaction
	result, err := transition.Write(tx)
	require.NoError(t, err)

	fmt.Printf("Logs: %v\n", result.Logs)
	fmt.Printf("Gas used: %d\n", result.GasUsed)

	// retrieve the state data changed
	objs := transition.Commit()

	// commit the data to the state
	sw, b := snap.Commit(objs)
	require.NotNil(t, sw)
	require.NotNil(t, b)
}
