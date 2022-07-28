package analysis

import (
	"encoding/csv"
	"fmt"
	"github.com/heimdalr/dag"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

type TxDeps struct {
	id        string
	readDeps  map[string]bool
	writeDeps map[string]bool
}

func (td *TxDeps) ID() string {
	return td.id
}

func (td *TxDeps) String() string {
	return td.id
}

func (td *TxDeps) addRead(r string) {
	if td.readDeps == nil {
		td.readDeps = make(map[string]bool)
	}
	td.readDeps[r] = true
}

func (td *TxDeps) addWrite(w string) {
	if td.writeDeps == nil {
		td.writeDeps = make(map[string]bool)
	}
	td.writeDeps[w] = true
}

func (td *TxDeps) hasDep(txTo *TxDeps) bool {
	for k, _ := range td.writeDeps {
		if txTo.readDeps[k] {
			return true
		}
	}
	return false
}

var _ dag.IDInterface = &TxDeps{}

func TestTransactionDAG(t *testing.T) {

	d := dag.NewDAG()
	_ = d

	ignoreDepPrefixes := []string{"742d13f0b2a19c823bdd362b16305e4704b97a38", "70bca57f4579f58670ab2d18ef16e02c17553c38"}

	// data_31212704
	f, err := os.Open("/Users/pauloleary/work/data_31218048.csv")
	require.NoError(t, err)

	cr := csv.NewReader(f)

	recs, err := cr.ReadAll()
	require.NoError(t, err)

	txs := make(map[string]*TxDeps)
	recs = recs[1:] // skip header

	checkIgnore := func(dep string) bool {
		for _, v := range ignoreDepPrefixes {
			if strings.HasPrefix(dep, v) {
				return true
			}
		}
		return false
	}

	for _, r := range recs {
		txId := r[0]
		dep := strings.TrimSpace(r[4])
		depType := strings.ToLower(strings.TrimSpace(r[5]))

		if checkIgnore(dep) {
			continue
		}

		var tx *TxDeps
		var ok bool
		if tx, ok = txs[txId]; !ok {
			tx = &TxDeps{id: txId}
			txs[txId] = tx
		}

		switch depType {
		case "read":
			{
				tx.addRead(dep)
			}
		case "write":
			{
				tx.addWrite(dep)
			}
		default:
			require.Fail(t, "invalid dep type")
		}
	}

	for i := 0; i < len(txs); i++ {
		txFrom := txs[fmt.Sprint(i)]
		fromId, _ := d.AddVertex(txFrom)
		for j := i + 1; j < len(txs); j++ {
			txTo := txs[fmt.Sprint(j)]
			if txFrom.hasDep(txTo) {
				toId, _ := d.AddVertex(txTo)
				d.AddEdge(fromId, toId)
			}
		}
	}

	d.ReduceTransitively()
	println(d.String())

	maxDesc := 0
	for i := 0; i < len(txs); i++ {
		desc, err := d.GetDescendants(fmt.Sprint(i))
		require.NoError(t, err)
		if len(desc) > maxDesc {
			maxDesc = len(desc)
		}
	}

	println(fmt.Sprintf("max chain length: %v", maxDesc))
}
