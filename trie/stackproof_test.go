package trie

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"golang.org/x/crypto/sha3"
	"golang.org/x/exp/slices"
)

func trieWithSmallValues() (*Trie, map[string]*kv) {
	trie := NewEmpty(NewDatabase(rawdb.NewMemoryDatabase(), nil))
	vals := make(map[string]*kv)
	// This loop creates a few dense nodes with small leafs: hence will
	// cause embedded nodes.
	for i := byte(0); i < 100; i++ {
		value := &kv{common.LeftPadBytes([]byte{i}, 32), []byte{i}, false}
		trie.MustUpdate(value.k, value.v)
		vals[string(value.k)] = value
	}
	return trie, vals
}

func TestStRangeProofLeftside(t *testing.T) {
	trie, vals := randomTrie(4096)
	testStRangeProofLeftside(t, trie, vals)
}

func TestStRangeProofLeftsideSmallValues(t *testing.T) {
	trie, vals := trieWithSmallValues()
	testStRangeProofLeftside(t, trie, vals)
}

func testStRangeProofLeftside(t *testing.T, trie *Trie, vals map[string]*kv) {
	var (
		want    = trie.Hash()
		entries []*kv
	)
	for _, kv := range vals {
		entries = append(entries, kv)
	}
	slices.SortFunc(entries, (*kv).cmp)
	for start := 10; start < len(vals); start *= 2 {
		// Set write-fn on both stacktries, to compare outputs
		var (
			haveSponge = &spongeDb{sponge: sha3.NewLegacyKeccak256(), id: "have"}
			wantSponge = &spongeDb{sponge: sha3.NewLegacyKeccak256(), id: "want"}
			proof      = memorydb.New()
		)
		// Provide the proof for the first entry
		if err := trie.Prove(entries[start].k, proof); err != nil {
			t.Fatalf("Failed to prove the first node %v", err)
		}
		// Initiate the stacktrie with the proof
		stTrie, err := newStackTrieFromProof(trie.Hash(), entries[start].k, proof, func(owner common.Hash, path []byte, hash common.Hash, blob []byte) {
			rawdb.WriteTrieNode(haveSponge, owner, path, hash, blob, "path")
		})
		if err != nil {
			t.Fatal(err)
		}
		// Initiate a reference stacktrie without proof (filling manually)
		refTrie := NewStackTrie(nil)
		for i := 0; i <= start; i++ { // do prefill
			k, v := common.CopyBytes(entries[i].k), common.CopyBytes(entries[i].v)
			refTrie.Update(k, v)
		}
		refTrie.writeFn = func(owner common.Hash, path []byte, hash common.Hash, blob []byte) {
			rawdb.WriteTrieNode(wantSponge, owner, path, hash, blob, "path")
		}
		// Feed the remaining values into them both
		for i := start + 1; i < len(vals); i++ {
			stTrie.Update(entries[i].k, common.CopyBytes(entries[i].v))
			refTrie.Update(entries[i].k, common.CopyBytes(entries[i].v))
		}
		// Verify the final trie hash
		if have := stTrie.Hash(); have != want {
			t.Fatalf("wrong hash, have %x want %x\n", have, want)
		}
		if have := refTrie.Hash(); have != want {
			t.Fatalf("wrong hash, have %x want %x\n", have, want)
		}
		// Verify the sequence of committed nodes
		if have, want := haveSponge.sponge.Sum(nil), wantSponge.sponge.Sum(nil); !bytes.Equal(have, want) {
			// Show the journal
			t.Logf("Want:")
			for i, v := range wantSponge.journal {
				t.Logf("op %d: %v", i, v)
			}
			t.Logf("Have:")
			for i, v := range haveSponge.journal {
				t.Logf("op %d: %v", i, v)
			}
			t.Errorf("proof from %d: disk write sequence wrong:\nhave %x want %x\n", start, have, want)
		}
	}
}
