package erigon_evm

import (
	"context"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
)

var testGasPrice = big.NewInt(10_000)
var testFromAddr = common.BytesToAddress([]byte{0xde, 0xad, 0xbe, 0xef})
var testToAddr = common.BytesToAddress([]byte{0xca, 0xfe, 0xba, 0xbe})

func makeTestMessage() types.Message {
	return types.NewMessage(testFromAddr, &testToAddr, 0, uint256.NewInt(100_000), 100_000,
		uint256.NewInt(50_000), uint256.NewInt(0), nil, nil, nil, false)
}

func TestErigonEVM(t *testing.T) {

	msg := makeTestMessage()

	db := ethdb.NewTestDB(t)
	r, w := state.NewPlainStateReader(db), state.NewPlainStateWriter(db, nil, 0)
	intraBlockState := state.New(r)

	tx := vm.TxContext{}
	evm := vm.NewEVM(vm.BlockContext{
		CheckTEVM:   func(common.Hash) (bool, error) { return false, nil },
		CanTransfer: func(blockState vm.IntraBlockState, address common.Address, u *uint256.Int) bool { return true },
		Transfer:    core.Transfer,
	}, tx, intraBlockState, params.TestChainConfig, vm.Config{})

	gp := core.GasPool(0)
	st := core.NewStateTransition(evm, msg, &gp)
	res, err := st.TransitionDb(true, true)
	require.NoError(t, err)
	require.NotNil(t, res)

	err = intraBlockState.CommitBlock(context.Background(), w)
	require.NoError(t, err)
}
