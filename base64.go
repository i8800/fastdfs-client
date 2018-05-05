package go_fastdfs

import (
	"bytes"
	"errors"
	"strings"
)

const (
	IGNORE    = -1
	PAD       = -2
	CH_PLUS   = '-'
	CH_SPLASH = '_'
	CH_PAD    = '.'
)

var (
	valueToChar [64]int16
	charToValue [256]int16
)

func decodeAuto(s string) ([]byte, error) {
	n := len(s) % 4
	if n != 0 {
		s += strings.Repeat(string(CH_PAD), 4-n)
	}
	return decode(s)
}

func decode(s string) ([]byte, error) {
	var (
		dummies  int
		combined int
		cycle    int
	)

	ln := len(s)
	var b bytes.Buffer
	for i := 0; i < ln; i++ {
		c := s[i]
		value := int(charToValue[c])
		switch value {
		case IGNORE:
			break
		case PAD:
			value = 0
			dummies++
			fallthrough
		default:
			switch cycle {
			case 0:
				combined = value
				cycle = 1
			case 1:
				combined <<= 6
				combined |= value
				cycle = 2
			case 2:
				combined <<= 6
				combined |= value
				cycle = 3
			case 3:
				combined <<= 6
				combined |= value
				b.WriteByte(byte(combined >> 16))
				b.WriteByte(byte((combined & 0x0000ff00) >> 8))
				b.WriteByte(byte(combined & 0x000000ff))
				cycle = 0
			}
		}
	}

	if cycle != 0 {
		return nil, errors.New("Input to decode not an even multiple of 4 characters.")
	}

	return b.Bytes(), nil
}

func init() {
	//A-Z...
	for i := 0; i <= 25; i++ {
		valueToChar[i] = int16('A' + i)
	}
	//a-z
	for i := 0; i <= 25; i++ {
		valueToChar[i+26] = int16('a' + i)
	}
	//0-9
	for i := 0; i <= 9; i++ {
		valueToChar[i+52] = int16('0' + i)
	}
	valueToChar[62] = CH_PLUS
	valueToChar[63] = CH_SPLASH

	for i := 0; i < 256; i++ {
		charToValue[i] = int16(IGNORE)
	}

	for i := 0; i < 64; i++ {
		charToValue[valueToChar[i]] = int16(i)
	}

	charToValue[CH_PAD] = PAD
}
