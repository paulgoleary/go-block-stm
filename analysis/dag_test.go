package analysis

import (
	"encoding/csv"
	"fmt"
	"github.com/heimdalr/dag"
	"github.com/stretchr/testify/require"
	"os"
	"sort"
	"strconv"
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

func (td *TxDeps) getForwardDeps(txTo *TxDeps) (ret map[string]bool) {
	ret = make(map[string]bool)
	for k, _ := range td.writeDeps {
		if txTo.readDeps[k] {
			ret[k] = true
		}
	}
	return
}

func (td *TxDeps) hasReadDep(txFrom *TxDeps) bool {
	for k, _ := range td.readDeps {
		if txFrom.writeDeps[k] {
			return true
		}
	}
	return false
}

var _ dag.IDInterface = &TxDeps{}

func TestTransactionDAG(t *testing.T) {

	d := dag.NewDAG()

	ignoreDepPrefixes := []string{"742d13f0b2a19c823bdd362b16305e4704b97a38", "70bca57f4579f58670ab2d18ef16e02c17553c38"}

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

	d.AddVertex(txs["0"]) // make sure 0 is added ...
	for i := len(txs) - 1; i > 0; i-- {
		txTo := txs[fmt.Sprint(i)]
		txToId, _ := d.AddVertex(txTo)
		for j := i - 1; j >= 0; j-- {
			txFrom := txs[fmt.Sprint(j)]
			if txFrom.hasReadDep(txTo) {
				txFromId, _ := d.AddVertex(txFrom)
				d.AddEdge(txFromId, txToId)
				break // once we add a 'backward' dep we can't execute before that transaction so no need to proceed
			}
		}
	}

	mustAtoI := func(s string) int {
		if i, err := strconv.Atoi(s); err != nil {
			panic(err)
		} else {
			return i
		}
	}

	maxDesc := 0
	var roots []int
	for k, _ := range d.GetRoots() {
		roots = append(roots, mustAtoI(k))
	}
	sort.Ints(roots)

	makeStrs := func(ints []int) (ret []string) {
		for _, v := range ints {
			ret = append(ret, fmt.Sprint(v))
		}
		return
	}

	for _, v := range roots {
		ids := []int{v}
		desc, _ := d.GetDescendants(fmt.Sprint(v))
		for kd, _ := range desc {
			ids = append(ids, mustAtoI(kd))
		}
		sort.Ints(ids)
		println(fmt.Sprintf("(%v) %v", len(ids), strings.Join(makeStrs(ids), "->")))

		if len(desc) > maxDesc {
			maxDesc = len(desc)
		}
	}

	println(fmt.Sprintf("max chain length: %v of %v", maxDesc+1, len(txs)))
}
