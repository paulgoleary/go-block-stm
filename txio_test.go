package block_stm

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWriteCompares(t *testing.T) {

	wd1 := WriteDescriptor{Path: []byte("1")}
	wd2 := WriteDescriptor{Path: []byte("2")}
	wd3 := WriteDescriptor{Path: []byte("3")}
	wd4 := WriteDescriptor{Path: []byte("4")}

	txOut0 := TxnOutput{}
	txOut1 := TxnOutput{wd1, wd2}
	txOut2 := TxnOutput{wd1, wd2}
	txOut3 := TxnOutput{wd1, wd2, wd3}
	txOut4 := TxnOutput{wd1, wd4, wd3}
	txOut5 := TxnOutput{wd1, wd2, wd3, wd4}

	require.False(t, txOut0.hasNewWrite(txOut1))
	require.False(t, txOut1.hasNewWrite(txOut2))

	require.True(t, txOut3.hasNewWrite(txOut0))
	require.True(t, txOut3.hasNewWrite(txOut1))
	require.True(t, txOut4.hasNewWrite(txOut3))

	require.False(t, txOut4.hasNewWrite(txOut5), "tx does not write to any *new* output paths")
}
