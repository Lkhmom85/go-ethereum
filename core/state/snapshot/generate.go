// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/ethdb/relaydb"
	"math/big"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyCode is the known hash of the empty EVM bytecode.
	emptyCode = crypto.Keccak256Hash(nil)

	// accountCheckRange is the upper limit of the number of accounts involved in
	// each range check. This is a value estimated based on experience. If this
	// value is too large, the failure rate of range prove will increase. Otherwise
	// the the value is too small, the efficiency of the state recovery will decrease.
	accountCheckRange = 128

	// storageCheckRange is the upper limit of the number of storage slots involved
	// in each range check. This is a value estimated based on experience. If this
	// value is too large, the failure rate of range prove will increase. Otherwise
	// the the value is too small, the efficiency of the state recovery will decrease.
	storageCheckRange = 1024
)

// Metrics in generation
var (
	snapGeneratedAccountMeter     = metrics.NewRegisteredMeter("state/snapshot/generation/account/generated", nil)
	snapRecoveredAccountMeter     = metrics.NewRegisteredMeter("state/snapshot/generation/account/recovered", nil)
	snapWipedAccountMeter         = metrics.NewRegisteredMeter("state/snapshot/generation/account/wiped", nil)
	snapMissallAccountMeter       = metrics.NewRegisteredMeter("state/snapshot/generation/account/missall", nil)
	snapGeneratedStorageMeter     = metrics.NewRegisteredMeter("state/snapshot/generation/storage/generated", nil)
	snapRecoveredStorageMeter     = metrics.NewRegisteredMeter("state/snapshot/generation/storage/recovered", nil)
	snapWipedStorageMeter         = metrics.NewRegisteredMeter("state/snapshot/generation/storage/wiped", nil)
	snapMissallStorageMeter       = metrics.NewRegisteredMeter("state/snapshot/generation/storage/missall", nil)
	snapSuccessfulRangeProofMeter = metrics.NewRegisteredMeter("state/snapshot/generation/proof/success", nil)
	snapFailedRangeProofMeter     = metrics.NewRegisteredMeter("state/snapshot/generation/proof/failure", nil)

	snapAccountProveTimer    = metrics.NewRegisteredGauge("state/snapshot/generation/duration/account/prove", nil)
	snapAccountTrieReadTimer = metrics.NewRegisteredGauge("state/snapshot/generation/duration/account/trieread", nil)
	snapAccountSnapReadTimer = metrics.NewRegisteredGauge("state/snapshot/generation/duration/account/snapread", nil)
	snapAccountWriteTimer    = metrics.NewRegisteredGauge("state/snapshot/generation/duration/account/write", nil)
	snapStorageProveTimer    = metrics.NewRegisteredGauge("state/snapshot/generation/duration/storage/prove", nil)
	snapStorageTrieReadTimer = metrics.NewRegisteredGauge("state/snapshot/generation/duration/storage/trieread", nil)
	snapStorageSnapReadTimer = metrics.NewRegisteredGauge("state/snapshot/generation/duration/storage/snapread", nil)
	snapStorageWriteTimer    = metrics.NewRegisteredGauge("state/snapshot/generation/duration/storage/write", nil)
)

// Global timer for metrics
var (
	accountProving  time.Duration // The total time spent on the account proving
	accountTrieRead time.Duration // The total time spent on the account trie iteration
	accountSnapRead time.Duration // The total time spent on the snapshot account iteration
	accountWrite    time.Duration // The total time spent on writing/updating/deleting accounts
	storageProving  time.Duration // The total time spent on the storage proving
	storageTrieRead time.Duration // The total time spent on the storage trie iteration
	storageSnapRead time.Duration // The total time spent on the snapshot storage iteration
	storageWrite    time.Duration // The total time spent on writing/updating/deleting storages
)

// generatorStats is a collection of statistics gathered by the snapshot generator
// for logging purposes.
type generatorStats struct {
	origin   uint64             // Origin prefix where generation started
	start    time.Time          // Timestamp when generation started
	accounts uint64             // Number of accounts indexed(generated or recovered)
	slots    uint64             // Number of storage slots indexed(generated or recovered)
	storage  common.StorageSize // Total account and storage slot size(generation or recovery)
}

// Log creates an contextual log with the given message and the context pulled
// from the internally maintained statistics.
func (gs *generatorStats) Log(msg string, root common.Hash, marker []byte) {
	var ctx []interface{}
	if root != (common.Hash{}) {
		ctx = append(ctx, []interface{}{"root", root}...)
	}
	// Figure out whether we're after or within an account
	switch len(marker) {
	case common.HashLength:
		ctx = append(ctx, []interface{}{"at", common.BytesToHash(marker)}...)
	case 2 * common.HashLength:
		ctx = append(ctx, []interface{}{
			"in", common.BytesToHash(marker[:common.HashLength]),
			"at", common.BytesToHash(marker[common.HashLength:]),
		}...)
	}
	// Add the usual measurements
	ctx = append(ctx, []interface{}{
		"accounts", gs.accounts,
		"slots", gs.slots,
		"storage", gs.storage,
		"elapsed", common.PrettyDuration(time.Since(gs.start)),
	}...)
	// Calculate the estimated indexing time based on current stats
	if len(marker) > 0 {
		if done := binary.BigEndian.Uint64(marker[:8]) - gs.origin; done > 0 {
			left := math.MaxUint64 - binary.BigEndian.Uint64(marker[:8])

			speed := done/uint64(time.Since(gs.start)/time.Millisecond+1) + 1 // +1s to avoid division by zero
			ctx = append(ctx, []interface{}{
				"eta", common.PrettyDuration(time.Duration(left/speed) * time.Millisecond),
			}...)
		}
	}
	log.Info(msg, ctx...)
}

// generateSnapshot regenerates a brand new snapshot based on an existing state
// database and head block asynchronously. The snapshot is returned immediately
// and generation is continued in the background until done.
func generateSnapshot(diskdb ethdb.KeyValueStore, triedb *trie.Database, cache int, root common.Hash) *diskLayer {
	// Create a new disk layer with an initialized state marker at zero
	var (
		stats     = &generatorStats{start: time.Now()}
		batch     = diskdb.NewBatch()
		genMarker = []byte{} // Initialized but empty!
	)
	rawdb.WriteSnapshotRoot(batch, root)
	journalProgress(batch, genMarker, stats)
	if err := batch.Write(); err != nil {
		log.Crit("Failed to write initialized state marker", "err", err)
	}
	base := &diskLayer{
		diskdb:     diskdb,
		triedb:     triedb,
		root:       root,
		cache:      fastcache.New(cache * 1024 * 1024),
		genMarker:  genMarker,
		genPending: make(chan struct{}),
		genAbort:   make(chan chan *generatorStats),
	}
	go base.generate(stats)
	log.Debug("Start snapshot generation", "root", root)
	return base
}

// journalProgress persists the generator stats into the database to resume later.
func journalProgress(db ethdb.KeyValueWriter, marker []byte, stats *generatorStats) {
	// Write out the generator marker. Note it's a standalone disk layer generator
	// which is not mixed with journal. It's ok if the generator is persisted while
	// journal is not.
	entry := journalGenerator{
		Done:   marker == nil,
		Marker: marker,
	}
	if stats != nil {
		entry.Accounts = stats.accounts
		entry.Slots = stats.slots
		entry.Storage = uint64(stats.storage)
	}
	blob, err := rlp.EncodeToBytes(entry)
	if err != nil {
		panic(err) // Cannot happen, here to catch dev errors
	}
	var logstr string
	switch {
	case marker == nil:
		logstr = "done"
	case bytes.Equal(marker, []byte{}):
		logstr = "empty"
	case len(marker) == common.HashLength:
		logstr = fmt.Sprintf("%#x", marker)
	default:
		logstr = fmt.Sprintf("%#x:%#x", marker[:common.HashLength], marker[common.HashLength:])
	}
	log.Debug("Journalled generator progress", "progress", logstr)
	rawdb.WriteSnapshotGenerator(db, blob)
}

// proofResult contains the output of range proving which can be used
// for further processing no matter it's successful or not.
type proofResult struct {
	keys     [][]byte   // The key set of all elements being iterated, even proving is failed
	vals     [][]byte   // The val set of all elements being iterated, even proving is failed
	diskMore bool       // Set when the database has extra snapshot states since last iteration
	trieMore bool       // Set when the trie has extra snapshot states(only meaningful for successful proving)
	proofErr error      // Indicator whether the given state range is valid or not
	tr       *trie.Trie // The trie, in case the trie was resolved by the prover (may be nil)
}

// valid returns the indicator that range proof is successful or not.
func (result *proofResult) valid() bool {
	return result.proofErr == nil
}

// last returns the last verified element key no matter the range proof is
// successful or not. Nil is returned if nothing involved in the proving.
func (result *proofResult) last() []byte {
	var last []byte
	if len(result.keys) > 0 {
		last = result.keys[len(result.keys)-1]
	}
	return last
}

// forEach iterates all the visited elements and applies the given callback on them.
// The iteration is aborted if the callback returns non-nil error.
func (result *proofResult) forEach(callback func(key []byte, val []byte) error) error {
	for i := 0; i < len(result.keys); i++ {
		key, val := result.keys[i], result.vals[i]
		if err := callback(key, val); err != nil {
			return err
		}
	}
	return nil
}

// proveRange proves the state segment with particular prefix is "valid".
// The iteration start point will be assigned if the iterator is restored from
// the last interruption. Max will be assigned in order to limit the maximum
// amount of data involved in each iteration.
//
// The proof result will be returned if the range proving is finished, otherwise
// the error will be returned to abort the entire procedure.
func (dl *diskLayer) proveRange(root common.Hash, prefix []byte, kind string, origin []byte, max int, valueConvertFn func([]byte) ([]byte, error)) (*proofResult, error) {
	var (
		keys     [][]byte
		vals     [][]byte
		proof    = rawdb.NewMemoryDatabase()
		diskMore = false
	)
	iter := dl.diskdb.NewIterator(prefix, origin)
	defer iter.Release()

	var start = time.Now()
	for iter.Next() {
		key := iter.Key()
		if len(key) != len(prefix)+common.HashLength {
			continue
		}
		if len(keys) == max {
			// Break if we've reached the max size, and signal that we're not
			// done yet.
			diskMore = true
			break
		}
		keys = append(keys, common.CopyBytes(key[len(prefix):]))

		if valueConvertFn == nil {
			vals = append(vals, common.CopyBytes(iter.Value()))
		} else {
			val, err := valueConvertFn(iter.Value())
			if err != nil {
				// Special case, the state data is corrupted (invalid slim-format account),
				// don't abort the entire procedure directly. Instead, let the fallback
				// generation to heal the invalid data.
				//
				// Here append the original value to ensure that the number of key and
				// value are the same.
				vals = append(vals, common.CopyBytes(iter.Value()))
			} else {
				vals = append(vals, val)
			}
		}
	}
	// Update metrics for database iteration and merkle proving
	if kind == "storage" {
		storageSnapRead += time.Since(start)
		snapStorageSnapReadTimer.Update(int64(storageSnapRead))
	} else {
		accountSnapRead += time.Since(start)
		snapAccountSnapReadTimer.Update(int64(accountSnapRead))
	}
	defer func(start time.Time) {
		if kind == "storage" {
			storageProving += time.Since(start)
			snapStorageProveTimer.Update(int64(storageProving))
		} else {
			accountProving += time.Since(start)
			snapAccountProveTimer.Update(int64(accountProving))
		}
	}(time.Now())

	// The snap state is exhausted, pass the entire key/val set for verification
	if origin == nil && !diskMore {
		stackTr := trie.NewStackTrie(nil)
		for i, key := range keys {
			stackTr.TryUpdate(key, common.CopyBytes(vals[i]))
		}
		if gotRoot := stackTr.Hash(); gotRoot != root {
			return &proofResult{keys: keys, vals: vals, diskMore: false, trieMore: false, proofErr: errors.New("wrong root")}, nil
		}
		return &proofResult{keys: keys, vals: vals, diskMore: false, trieMore: false, proofErr: nil}, nil
	}
	// Snap state is chunked, generate edge proofs for verification.
	tr, err := trie.New(root, dl.triedb)
	if err != nil {
		log.Error("Missing trie", "root", root, "err", err)
		return nil, err
	}
	// Firstly find out the key of last iterated element.
	var last []byte
	if len(keys) > 0 {
		last = keys[len(keys)-1]
	}
	// Generate the Merkle proofs for the first and last element
	if origin == nil {
		origin = common.Hash{}.Bytes()
	}
	if err := tr.Prove(origin, 0, proof); err != nil {
		log.Debug("Failed to prove range", "kind", kind, "origin", origin, "err", err)
		return &proofResult{keys: keys, vals: vals, diskMore: diskMore, trieMore: false, proofErr: err, tr: tr}, nil
	}
	if last != nil {
		if err := tr.Prove(last, 0, proof); err != nil {
			log.Debug("Failed to prove range", "kind", kind, "last", last, "err", err)
			return &proofResult{keys: keys, vals: vals, diskMore: diskMore, trieMore: false, proofErr: err, tr: tr}, nil
		}
	}
	// Verify the state segment with range prover, ensure that all flat states
	// in this range correspond to merkle trie.
	_, _, _, cont, err := trie.VerifyRangeProof(root, origin, last, keys, vals, proof)
	return &proofResult{keys: keys, vals: vals, diskMore: diskMore, trieMore: cont, proofErr: err, tr: tr}, nil
}

// onStateCallback is a function that is called by generateRange, when processing a range of
// accounts or storage slots. For each element, the callback is invoked.
// If 'delete' is true, then this element (and potential slots) needs to be deleted from the snapshot.
// If 'write' is true, then this element needs to be updated with the 'val'.
// If 'write' is false, then this element is already correct, and needs no update. However,
// for accounts, the storage trie of the account needs to be checked.
// The 'val' is the canonical encoding of the value (not the slim format for accounts)
type onStateCallback func(key []byte, val []byte, write bool, delete bool) error

// generateRange generates the state segment with particular prefix. Generation can
// either verify the correctness of existing state through rangeproof and skip
// generation, or iterate trie to regenerate state on demand.
func (dl *diskLayer) generateRange(root common.Hash, prefix []byte, kind string, origin []byte, max int, stats *generatorStats, onState onStateCallback, valueConvertFn func([]byte) ([]byte, error)) (bool, []byte, error) {
	// Use range prover to check the validity of the flat state in the range
	result, err := dl.proveRange(root, prefix, kind, origin, max, valueConvertFn)
	if err != nil {
		return false, nil, err
	}
	last := result.last()

	// Construct contextual logger
	logCtx := []interface{}{"kind", kind, "prefix", hexutil.Encode(prefix)}
	if len(origin) > 0 {
		logCtx = append(logCtx, "origin", hexutil.Encode(origin))
	}
	logger := log.New(logCtx...)

	// The range prover says the range is correct, skip trie iteration
	if result.valid() {
		snapSuccessfulRangeProofMeter.Mark(1)
		logger.Trace("Proved state range", "last", hexutil.Encode(last))

		// The verification is passed, process each state with the given
		// callback function. If this state represents a contract, the
		// corresponding storage check will be performed in the callback
		if err := result.forEach(func(key []byte, val []byte) error { return onState(key, val, false, false) }); err != nil {
			return false, nil, err
		}
		// Only abort the iteration when both database and trie are exhausted
		return !result.diskMore && !result.trieMore, last, nil
	}
	logger.Trace("Detected outdated state range", "last", hexutil.Encode(last), "err", result.proofErr)
	snapFailedRangeProofMeter.Mark(1)

	// Special case, the entire trie is missing. In the original trie scheme,
	// all the duplicated subtries will be filter out(only one copy of data
	// will be stored). While in the snapshot model, all the storage tries
	// belong to different contracts will be kept even they are duplicated.
	// Track it to a certain extent remove the noise data used for statistics.
	if origin == nil && last == nil {
		meter := snapMissallAccountMeter
		if kind == "storage" {
			meter = snapMissallStorageMeter
		}
		meter.Mark(1)
	}

	tr := result.tr

	// We use the snap data to build up a cache which can be used by the
	// main account trie as a primary lookup when resolving hashes

	if len(result.keys) > 0 {
		snapNodeCache := memorydb.New()
		snapTrieDb := trie.NewDatabase(snapNodeCache)
		snapTrie, _ := trie.New(common.Hash{}, snapTrieDb)
		for i, key := range result.keys {
			snapTrie.Update(key, result.vals[i])
		}
		snapRoot, _ := snapTrie.Commit(nil)
		snapTrieDb.Commit(snapRoot, false, nil)
		// The memory db should now hold all leaves in their hashed form
		relay := relaydb.New(snapNodeCache, dl.diskdb)
		defer func() {
			primHits, relays := relay.Efficiency()
			log.Info("Snapnodecache used", "primaries", primHits, "relays", relays)
		}()
		triedb := trie.NewDatabase(relay)
		tr, _ = trie.New(root, triedb)
	}

	if tr == nil {
		tr, err = trie.New(root, dl.triedb)
		if err != nil {
			return false, nil, err
		}
	}

	var (
		trieMore       bool
		iter           = trie.NewIterator(tr.NodeIterator(origin))
		kvkeys, kvvals = result.keys, result.vals

		// counters
		count     = 0 // number of states delivered by iterator
		created   = 0 // states created from the trie
		updated   = 0 // states updated from the trie
		deleted   = 0 // states not in trie, but were in snapshot
		untouched = 0 // states already correct

		// timers
		start    = time.Now()
		istart   time.Time
		internal time.Duration
	)
	for iter.Next() {
		if last != nil && bytes.Compare(iter.Key, last) > 0 {
			trieMore = true
			break
		}
		count += 1

		write := true
		created++
		for len(kvkeys) > 0 {
			if cmp := bytes.Compare(kvkeys[0], iter.Key); cmp < 0 {
				// delete the key
				istart = time.Now()
				if err := onState(kvkeys[0], nil, false, true); err != nil {
					return false, nil, err
				}
				kvkeys = kvkeys[1:]
				kvvals = kvvals[1:]
				deleted += 1
				internal += time.Since(istart)
				continue
			} else if cmp == 0 {
				// the snapshot key can be overwritten
				created--
				if write = !bytes.Equal(kvvals[0], iter.Value); write {
					updated++
				} else {
					untouched++
				}
				kvkeys = kvkeys[1:]
				kvvals = kvvals[1:]
			}
			break
		}
		istart = time.Now()
		if err := onState(iter.Key, iter.Value, write, false); err != nil {
			return false, nil, err
		}
		internal += time.Since(istart)
	}
	if iter.Err != nil {
		return false, nil, iter.Err
	}
	// Delete all stale snapshot states remaining
	istart = time.Now()
	for _, key := range kvkeys {
		if err := onState(key, nil, false, true); err != nil {
			return false, nil, err
		}
		deleted += 1
	}
	internal += time.Since(istart)

	// Update metrics for counting trie iteration
	if kind == "storage" {
		storageTrieRead += time.Since(start) - internal
		snapStorageTrieReadTimer.Update(int64(storageTrieRead))
	} else {
		accountTrieRead += time.Since(start) - internal
		snapAccountTrieReadTimer.Update(int64(accountTrieRead))
	}
	logger.Debug("Regenerated state range", "root", root, "last", hexutil.Encode(last),
		"count", count, "created", created, "updated", updated, "untouched", untouched, "deleted", deleted)

	// If there are either more trie items, or there are more snap items
	// (in the next segment), then we need to keep working
	return !trieMore && !result.diskMore, last, nil
}

// generate is a background thread that iterates over the state and storage tries,
// constructing the state snapshot. All the arguments are purely for statistics
// gathering and logging, since the method surfs the blocks as they arrive, often
// being restarted.
func (dl *diskLayer) generate(stats *generatorStats) {
	var (
		accMarker    []byte
		accountRange = accountCheckRange
	)
	if len(dl.genMarker) > 0 { // []byte{} is the start, use nil for that
		// Always reset the initial account range as 1
		// whenever recover from the interruption.
		accMarker, accountRange = dl.genMarker[:common.HashLength], 1
	}
	var (
		batch     = dl.diskdb.NewBatch()
		logged    = time.Now()
		accOrigin = common.CopyBytes(accMarker)
		abort     chan *generatorStats
	)
	stats.Log("Resuming state snapshot generation", dl.root, dl.genMarker)

	checkAndFlush := func(currentLocation []byte) error {
		select {
		case abort = <-dl.genAbort:
		default:
		}
		if batch.ValueSize() > ethdb.IdealBatchSize || abort != nil {
			// Flush out the batch anyway no matter it's empty or not.
			// It's possible that all the states are recovered and the
			// generation indeed makes progress.
			journalProgress(batch, currentLocation, stats)

			if err := batch.Write(); err != nil {
				return err
			}
			batch.Reset()

			dl.lock.Lock()
			dl.genMarker = currentLocation
			dl.lock.Unlock()

			if abort != nil {
				stats.Log("Aborting state snapshot generation", dl.root, currentLocation)
				return errors.New("aborted")
			}
		}
		if time.Since(logged) > 8*time.Second {
			stats.Log("Generating state snapshot", dl.root, currentLocation)
			logged = time.Now()
		}
		return nil
	}

	onAccount := func(key []byte, val []byte, write bool, delete bool) error {
		var (
			start       = time.Now()
			accountHash = common.BytesToHash(key)
		)
		if delete {
			rawdb.DeleteAccountSnapshot(batch, accountHash)
			snapWipedAccountMeter.Mark(1)

			// Ensure that any previous snapshot storage values are cleared
			prefix := append(rawdb.SnapshotStoragePrefix, accountHash.Bytes()...)
			keyLen := len(rawdb.SnapshotStoragePrefix) + 2*common.HashLength
			if err := wipeKeyRange(dl.diskdb, "storage", prefix, nil, nil, keyLen, snapWipedStorageMeter, false); err != nil {
				return err
			}
			accountWrite += time.Since(start)
			snapAccountWriteTimer.Update(int64(accountWrite))
			return nil
		}
		// Retrieve the current account and flatten it into the internal format
		var acc struct {
			Nonce    uint64
			Balance  *big.Int
			Root     common.Hash
			CodeHash []byte
		}
		if err := rlp.DecodeBytes(val, &acc); err != nil {
			log.Crit("Invalid account encountered during snapshot creation", "err", err)
		}
		// If the account is not yet in-progress, write it out
		if accMarker == nil || !bytes.Equal(accountHash[:], accMarker) {
			dataLen := len(val) // Approximate size, saves us a round of RLP-encoding
			if !write {
				if bytes.Equal(acc.CodeHash, emptyCode[:]) {
					dataLen -= 32
				}
				if acc.Root == emptyRoot {
					dataLen -= 32
				}
				snapRecoveredAccountMeter.Mark(1)
			} else {
				data := SlimAccountRLP(acc.Nonce, acc.Balance, acc.Root, acc.CodeHash)
				dataLen = len(data)
				rawdb.WriteAccountSnapshot(batch, accountHash, data)
				snapGeneratedAccountMeter.Mark(1)
			}
			stats.storage += common.StorageSize(1 + common.HashLength + dataLen)
			stats.accounts++
		}
		// If we've exceeded our batch allowance or termination was requested, flush to disk
		if err := checkAndFlush(accountHash[:]); err != nil {
			return err
		}
		// If the iterated account is the contract, create a further loop to
		// verify or regenerate the contract storage.
		if acc.Root == emptyRoot {
			// If the root is empty, we still need to ensure that any previous snapshot
			// storage values are cleared
			// TODO: investigate if this can be avoided, this will be very costly since it
			// affects every single EOA account
			//  - Perhaps we can avoid if where codeHash is emptyCode
			prefix := append(rawdb.SnapshotStoragePrefix, accountHash.Bytes()...)
			keyLen := len(rawdb.SnapshotStoragePrefix) + 2*common.HashLength
			if err := wipeKeyRange(dl.diskdb, "storage", prefix, nil, nil, keyLen, snapWipedStorageMeter, false); err != nil {
				return err
			}
			accountWrite += time.Since(start)
			snapAccountWriteTimer.Update(int64(accountWrite))
		} else {
			accountWrite += time.Since(start)
			snapAccountWriteTimer.Update(int64(accountWrite))

			var storeMarker []byte
			if accMarker != nil && bytes.Equal(accountHash[:], accMarker) && len(dl.genMarker) > common.HashLength {
				storeMarker = dl.genMarker[common.HashLength:]
			}
			onStorage := func(key []byte, val []byte, write bool, delete bool) error {
				defer func(start time.Time) {
					storageWrite += time.Since(start)
					snapStorageWriteTimer.Update(int64(storageWrite))
				}(time.Now())

				if delete {
					rawdb.DeleteStorageSnapshot(batch, accountHash, common.BytesToHash(key))
					snapWipedStorageMeter.Mark(1)
					return nil
				}
				if write {
					rawdb.WriteStorageSnapshot(batch, accountHash, common.BytesToHash(key), val)
					snapGeneratedStorageMeter.Mark(1)
				} else {
					snapRecoveredStorageMeter.Mark(1)
				}
				stats.storage += common.StorageSize(1 + 2*common.HashLength + len(val))
				stats.slots++

				// If we've exceeded our batch allowance or termination was requested, flush to disk
				if err := checkAndFlush(append(accountHash[:], key...)); err != nil {
					return err
				}
				return nil
			}
			var storeOrigin = common.CopyBytes(storeMarker)
			for {
				exhausted, last, err := dl.generateRange(acc.Root, append(rawdb.SnapshotStoragePrefix, accountHash.Bytes()...), "storage", storeOrigin, storageCheckRange, stats, onStorage, nil)
				if err != nil {
					return err
				}
				if exhausted {
					break
				}
				if storeOrigin = increaseKey(last); storeOrigin == nil {
					break // special case, the last is 0xffffffff...fff
				}
			}
		}
		// Some account processed, unmark the marker
		accMarker = nil
		return nil
	}

	// Global loop for regerating the entire state trie + all layered storage tries.
	for {
		exhausted, last, err := dl.generateRange(dl.root, rawdb.SnapshotAccountPrefix, "account", accOrigin, accountRange, stats, onAccount, FullAccountRLP)
		// The procedure it aborted, either by external signal or internal error
		if err != nil {
			if abort == nil { // aborted by internal error, wait the signal
				abort = <-dl.genAbort
			}
			abort <- stats
			return
		}
		// Abort the procedure if the entire snapshot is generated
		if exhausted {
			break
		}
		if accOrigin = increaseKey(last); accOrigin == nil {
			break // special case, the last is 0xffffffff...fff
		}
		accountRange = accountCheckRange
	}
	// Snapshot fully generated, set the marker to nil.
	// Note even there is nothing to commit, persist the
	// generator anyway to mark the snapshot is complete.
	journalProgress(batch, nil, stats)
	if err := batch.Write(); err != nil {
		log.Error("Failed to flush batch", "err", err)

		abort = <-dl.genAbort
		abort <- stats
		return
	}
	batch.Reset()

	log.Info("Generated state snapshot", "accounts", stats.accounts, "slots", stats.slots,
		"storage", stats.storage, "elapsed", common.PrettyDuration(time.Since(stats.start)))

	dl.lock.Lock()
	dl.genMarker = nil
	close(dl.genPending)
	dl.lock.Unlock()

	// Someone will be looking for us, wait it out
	abort = <-dl.genAbort
	abort <- nil
}

// increaseKey increase the input key by one bit. Return nil if the entire
// addition operation overflows,
func increaseKey(key []byte) []byte {
	for i := len(key) - 1; i >= 0; i-- {
		key[i]++
		if key[i] != 0x0 {
			return key
		}
	}
	return nil
}
