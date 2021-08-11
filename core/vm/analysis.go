// Copyright 2014 The go-ethereum Authors
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

package vm

// bitvec is a bit vector which maps bytes in a program.
// An unset bit means the byte is an opcode, a set bit means
// it's data (i.e. argument of PUSHxx).
type bitvec []byte

var lookup = [8]byte{
	0x80, 0x40, 0x20, 0x10, 0x8, 0x4, 0x2, 0x1,
}

func (bits *bitvec) set(pos uint64) {
	//(*bits)[pos/8] |= 0x80 >> (pos % 8)
	(*bits)[pos/8] |= lookup[pos%8]
}

func (bits *bitvec) set2(pos uint64) {
	(*bits)[pos/8+1] |= 0b1100_0000 << (8 - pos%8)
	(*bits)[pos/8] |= 0b1100_0000 >> (pos % 8)
}

func (bits *bitvec) set3(pos uint64) {
	(*bits)[pos/8+1] |= 0b1110_0000 << (8 - pos%8)
	(*bits)[pos/8] |= 0b1110_0000 >> (pos % 8)
}

func (bits *bitvec) set4(pos uint64) {
	(*bits)[pos/8+1] |= 0b1111_0000 << (8 - pos%8)
	(*bits)[pos/8] |= 0b1111_0000 >> (pos % 8)
}

func (bits *bitvec) set5(pos uint64) {
	(*bits)[pos/8+1] |= 0b1111_1000 << (8 - pos%8)
	(*bits)[pos/8] |= 0b1111_1000 >> (pos % 8)
}

func (bits *bitvec) set6(pos uint64) {
	(*bits)[pos/8+1] |= 0b1111_1100 << (8 - pos%8)
	(*bits)[pos/8] |= 0b1111_1100 >> (pos % 8)
}

func (bits *bitvec) set7(pos uint64) {
	(*bits)[pos/8+1] |= 0b1111_1110 << (8 - pos%8)
	(*bits)[pos/8] |= 0b1111_1110 >> (pos % 8)
}

func (bits *bitvec) set8(pos uint64) {
	(*bits)[pos/8+1] |= ^(0xFF >> (pos % 8))
	(*bits)[pos/8] |= 0xFF >> (pos % 8)
}

// codeSegment checks if the position is in a code segment.
func (bits *bitvec) codeSegment(pos uint64) bool {
	return ((*bits)[pos/8] & (0x80 >> (pos % 8))) == 0
}

// codeBitmap collects data locations in code.
func codeBitmap(code []byte) bitvec {
	// The bitmap is 4 bytes longer than necessary, in case the code
	// ends with a PUSH32, the algorithm will push zeroes onto the
	// bitvector outside the bounds of the actual code.
	bits := make(bitvec, len(code)/8+1+4)
	return codeBitmapInternal(code, bits)
}

// codeBitmapInternal is the internal implementation of codeBitmap.
// It exists for the purpose of being able to run benchmark tests
// without dynamic allocations affecting the results.
func codeBitmapInternal(code, bits bitvec) bitvec {
	for pc := uint64(0); pc < uint64(len(code)); {
		op := OpCode(code[pc])
		pc++
		if op < PUSH1 || op > PUSH32 {
			continue
		}
		numbits := op - PUSH1 + 1
		for ; numbits >= 8; numbits -= 8 {
			bits.set8(pc) // 8
			pc += 8
		}
		// numbits now max 7
		switch numbits {
		case 1:
			bits.set(pc)
		case 2:
			bits.set2(pc)
		case 3:
			bits.set3(pc)
		case 4:
			bits.set4(pc)
		case 5:
			bits.set5(pc)
		case 6:
			bits.set6(pc)
		case 7:
			bits.set7(pc)
		}
		pc += uint64(numbits)
	}
	return bits
}
