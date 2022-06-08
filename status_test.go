package block_stm

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStatusBasics(t *testing.T) {

	s := makeStatusManager(10)

	x := s.takeNextPending()
	require.Equal(t, 0, x)
	require.True(t, s.checkInProgress(x))

	x = s.takeNextPending()
	require.Equal(t, 1, x)

	s.markComplete(0)
	require.False(t, s.checkInProgress(0))
	require.Equal(t, 0, s.maxAllComplete())

	x = s.takeNextPending()
	require.Equal(t, 2, x)

	s.markComplete(x)
	require.False(t, s.checkInProgress(2))
	require.Equal(t, 0, s.maxAllComplete(), "zero should still be min complete")
}

func TestMaxComplete(t *testing.T) {

	s := makeStatusManager(10)

	for {
		tx := s.takeNextPending()
		if tx == -1 {
			break
		}
		if tx != 7 {
			s.markComplete(tx)
		}
	}

	require.Equal(t, 6, s.maxAllComplete())

	s2 := makeStatusManager(10)
	for {
		tx := s2.takeNextPending()
		if tx == -1 {
			break
		}
	}
	s2.markComplete(2)
	s2.markComplete(4)
	require.Equal(t, -1, s2.maxAllComplete())

	s2.complete = insertInList(s2.complete, 4)
	require.Equal(t, 2, s2.countComplete())
}
