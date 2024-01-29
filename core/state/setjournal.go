// Copyright 2024 The go-ethereum Authors
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

package state

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

type Journal interface {
	JournalCreate(addr common.Address)
	JournalTouch(addr common.Address, account *types.StateAccount, destructed bool)
	JournalNonceChange(addr common.Address, account *types.StateAccount, destructed bool)
	JournalBalanceChange(addr common.Address, account *types.StateAccount, destructed bool)
	JournalDestruct(addr common.Address, account *types.StateAccount)

	// JournalSetCode journals the setting of code: it is implicit that the previous
	// values were "no code" and emptyCodeHash and not destructed.
	JournalSetCode(addr common.Address, account *types.StateAccount)

	JournalLog(txHash common.Hash)
	JournalAccessListAddAccount(addr common.Address)
	JournalAccessListAddSlot(addr common.Address, slot common.Hash)
	JournalSetState(addr common.Address, key, prev common.Hash)
	JournalSetTransientState(addr common.Address, key, prev common.Hash)

	// Snapshot returns an identifier for the current revision of the state.
	// The lifeycle of journalling is as follows:
	// - Snapshot() starts a 'scope'.
	// - Tee method Snapshot() may be called any number of times.
	// - For each call to Snapshot, there should be a corresponding call to end
	//  the scope via either of:
	//   - RevertToSnapshot, which undoes the changes in the scope, or
	//   - DiscardSnapshot, which discards the ability to revert the changes in the scope.
	//     - This operation might merge the changes into the parent scope.
	//       If it does not merge the changes into the parent scope, it must create
	//       a new snapshot internally, in order to ensure that order of changes
	//       remains intact.
	Snapshot() int
	// RevertToSnapshot reverts all state changes made since the given revision.
	RevertToSnapshot(id int, s *StateDB)
	// DiscardSnapshot removes the snapshot. 	DiscardSnapshot(id int, s *StateDB)

	// Reset clears the journal, after this operation the journal can be used
	// anew. It is semantically similar to calling 'newJournal'.
	Reset()
}

var (
	_ Journal = (*sparseJournal)(nil)
)

// journalAccount represents the 'journable state' of a types.Account.
// Which means, all the normal fields except storage root, but also with a
// destruction-flag.
type journalAccount struct {
	nonce      uint64
	balance    uint256.Int
	codeHash   []byte // nil == emptyCodeHAsh
	destructed bool
}

type addrSlot struct {
	addr common.Address
	slot common.Hash
}

// scopedJournal represents all changes within a single callscope. These changes
// are either all reverted, or all committed -- they cannot be partially applied.
type scopedJournal struct {
	accountChanges map[common.Address]*journalAccount
	refund         int64
	logs           []common.Hash

	accessListAddresses []common.Address
	accessListAddrSlots []addrSlot

	storageChanges  map[common.Address]map[common.Hash]common.Hash
	tStorageChanges map[common.Address]map[common.Hash]common.Hash
}

func newScopedJournal() *scopedJournal {
	return &scopedJournal{
		refund: -1,
	}
}

func (j *scopedJournal) JournalRefund(prev uint64) {
	if j.refund == -1 {
		// We convert from uint64 to int64 here, so that we can use -1
		// to represent "no previous value set".
		// Treating refund as int64 is fine, there's no possibility for
		// refund to ever exceed maxInt64.
		j.refund = int64(prev)
	}
}

// journalAccountChange is the common shared implementation for all account-changes.
// These changes all fall back to this method:
// - balance change
// - nonce change
// - destruct-change
// - creation change (in this case, the account is nil)
func (j *scopedJournal) journalAccountChange(address common.Address, account *types.StateAccount, destructed bool) bool {
	// Unless the account has already been journalled, journal it now
	if _, ok := j.accountChanges[address]; !ok {
		if account != nil {
			ja := &journalAccount{
				nonce:   account.Nonce,
				balance: *account.Balance,
			}
			j.accountChanges[address] = ja
			if !bytes.Equal(account.CodeHash, types.EmptyCodeHash[:]) {
				ja.codeHash = account.CodeHash
			}
			ja.destructed = destructed
		} else {
			j.accountChanges[address] = nil
		}
		return true
	}
	return false
}

func (j *scopedJournal) journalLog(txHash common.Hash) {
	j.logs = append(j.logs, txHash)
}

func (j *scopedJournal) journalAccessListAddAccount(addr common.Address) {
	j.accessListAddresses = append(j.accessListAddresses, addr)
}

func (j *scopedJournal) journalAccessListAddSlot(addr common.Address, slot common.Hash) {
	j.accessListAddrSlots = append(j.accessListAddrSlots, addrSlot{addr, slot})
}

func (j *scopedJournal) journalSetState(addr common.Address, key, prev common.Hash) {
	if j.storageChanges == nil {
		j.storageChanges = make(map[common.Address]map[common.Hash]common.Hash)
	}
	changes, ok := j.storageChanges[addr]
	if !ok {
		changes = make(map[common.Hash]common.Hash)
		j.storageChanges[addr] = changes
	}
	// Do not overwrite a previous value!
	if _, ok := changes[key]; !ok {
		changes[key] = prev
	}
}

func (j *scopedJournal) journalSetTransientState(addr common.Address, key, prev common.Hash) {
	if j.tStorageChanges == nil {
		j.tStorageChanges = make(map[common.Address]map[common.Hash]common.Hash)
	}
	changes, ok := j.tStorageChanges[addr]
	if !ok {
		changes = make(map[common.Hash]common.Hash)
		j.tStorageChanges[addr] = changes
	}
	// Do not overwrite a previous value!
	if _, ok := changes[key]; !ok {
		changes[key] = prev
	}
}

func (j *scopedJournal) revert(s *StateDB, dirties map[common.Address]int) {
	// Revert refund
	if j.refund != -1 {
		s.refund = uint64(j.refund)
	}
	// Revert changes to accounts
	for addr, data := range j.accountChanges {
		if data == nil { // Reverting a create
			delete(s.stateObjects, addr)
			delete(s.stateObjectsDirty, addr)
			continue
		}
		obj := s.getStateObject(addr)
		obj.setNonce(data.nonce)
		// Setting 'code' to nil means it will be loaded from disk
		// next time it is needed. We avoid nilling it unless required
		journalHash := data.codeHash
		if data.codeHash == nil {
			if !bytes.Equal(obj.CodeHash(), types.EmptyCodeHash[:]) {
				obj.setCode(types.EmptyCodeHash, nil)
			}
		} else {
			if !bytes.Equal(obj.CodeHash(), journalHash) {
				obj.setCode(common.BytesToHash(data.codeHash), nil)
			}
		}
		obj.setBalance(&data.balance)
		obj.selfDestructed = data.destructed
		if dirties[addr]--; dirties[addr] == 0 {
			delete(dirties, addr)
		}
	}
	// Revert logs
	for _, txhash := range j.logs {
		logs := s.logs[txhash]
		if len(logs) == 1 {
			delete(s.logs, txhash)
		} else {
			s.logs[txhash] = logs[:len(logs)-1]
		}
		s.logSize--
	}
	// Revert access list additions
	for _, item := range j.accessListAddrSlots {
		s.accessList.DeleteSlot(item.addr, item.slot)
	}
	for _, item := range j.accessListAddresses {
		s.accessList.DeleteAddress(item)
	}

	// Revert storage changes
	for addr, changes := range j.storageChanges {
		obj := s.getStateObject(addr)
		for key, val := range changes {
			obj.setState(key, val)
		}
	}
	// Revert t-store changes
	for addr, changes := range j.tStorageChanges {
		for key, val := range changes {
			s.setTransientState(addr, key, val)
		}
	}
}

// sparseJournal contains the list of state modifications applied since the last state
// commit. These are tracked to be able to be reverted in the case of an execution
// exception or request for reversal.
type sparseJournal struct {
	entries []*scopedJournal       // Current changes tracked by the journal
	dirties map[common.Address]int // Dirty accounts and the number of changes

}

// newJournal creates a new initialized journal.
func newSparseJournal() *sparseJournal {
	return &sparseJournal{
		dirties: make(map[common.Address]int),
	}
}

// Reset clears the journal, after this operation the journal can be used
// anew. It is semantically similar to calling 'newJournal', but the underlying
// slices can be reused
func (j *sparseJournal) Reset() {
	j.entries = j.entries[:0]
	j.dirties = make(map[common.Address]int)
}

// Snapshot returns an identifier for the current revision of the state.
// OBS: A call to Snapshot is _required_ in order to initialize the journalling,
// invoking the journal-methods without having invoked Snapshot will lead to
// panic.
func (j *sparseJournal) Snapshot() int {
	id := len(j.entries)
	j.entries = append(j.entries, newScopedJournal())
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (j *sparseJournal) RevertToSnapshot(id int, s *StateDB) {
	if id >= len(j.entries) {
		panic(fmt.Errorf("revision id %v cannot be reverted", id))
	}
	// Revert the entries sequentially
	for i := id; i > 0; i-- {
		entry := j.entries[i]
		entry.revert(s, j.dirties)
	}
	j.entries = j.entries[:id]
}

func (j *sparseJournal) JournalReset(address common.Address,
	prev *stateObject,
	prevdestruct bool,
	prevAccount []byte,
	prevStorage map[common.Hash][]byte,
	prevAccountOriginExist bool,
	prevAccountOrigin []byte,
	prevStorageOrigin map[common.Hash][]byte) {
	panic("Not implemented")
}

func (j *sparseJournal) journalAccountChange(addr common.Address, account *types.StateAccount, destructed bool) {
	if j.entries[len(j.entries)-1].journalAccountChange(addr, account, destructed) {
		j.dirties[addr]++
	}
}

func (j *sparseJournal) JournalNonceChange(addr common.Address, account *types.StateAccount, destructed bool) {
	j.journalAccountChange(addr, account, destructed)
}

func (j *sparseJournal) JournalBalanceChange(addr common.Address, account *types.StateAccount, destructed bool) {
	j.journalAccountChange(addr, account, destructed)
}

func (j *sparseJournal) JournalSetCode(addr common.Address, account *types.StateAccount) {
	j.journalAccountChange(addr, account, false)
}

func (j *sparseJournal) JournalCreate(addr common.Address) {
	// Creating an account which is destructed, hence already exists, is not
	// allowed, hence we know it to be 'false'.
	j.journalAccountChange(addr, nil, false)
}

func (j *sparseJournal) JournalDestruct(addr common.Address, account *types.StateAccount) {
	// destructing an already destructed account must not be journalled. Hence we
	// know it to be 'false'.
	j.journalAccountChange(addr, account, false)
}

// var ripemd = common.HexToAddress("0000000000000000000000000000000000000003")
func (j *sparseJournal) JournalTouch(addr common.Address, account *types.StateAccount, destructed bool) {
	j.journalAccountChange(addr, account, destructed)
	if addr == ripemd {
		// Explicitly put it in the dirty-cache one extra time. Ripe magic.
		j.dirties[addr]++
	}
}

func (j *sparseJournal) JournalLog(txHash common.Hash) {
	j.entries[len(j.entries)-1].journalLog(txHash)
}

func (j *sparseJournal) JournalAccessListAddAccount(addr common.Address) {
	j.entries[len(j.entries)-1].journalAccessListAddAccount(addr)
}

func (j *sparseJournal) JournalAccessListAddSlot(addr common.Address, slot common.Hash) {
	j.entries[len(j.entries)-1].journalAccessListAddSlot(addr, slot)
}

func (j *sparseJournal) JournalSetState(addr common.Address, key, prev common.Hash) {
	j.entries[len(j.entries)-1].journalSetState(addr, key, prev)
}

func (j *sparseJournal) JournalSetTransientState(addr common.Address, key, prev common.Hash) {
	j.entries[len(j.entries)-1].journalSetTransientState(addr, key, prev)
}
