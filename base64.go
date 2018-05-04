package go_fastdfs

import (
	"errors"
	"fmt"
	"strings"
)

const (
	IGNORE    = -1
	PAD       = -2
	CH_PLUS   = '+'
	CH_SPLASH = '/'
	CH_PAD    = '='
)

var (
	valueToChar [64]int8
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
	fmt.Println(s, len(s))
	var (
		dummies  int
		combined uint
		cycle    int
		j        int
	)

	ln := len(s)
	b := make([]byte, ln/4*3)
	for i := 0; i < ln; i++ {
		c := s[i]
		value := IGNORE
		if c <= 255 {
			value = int(charToValue[c])
		}

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
				combined = uint(value)
				cycle = 1
			case 1:
				combined <<= 6
				combined |= uint(value)
				cycle = 2
			case 2:
				combined <<= 6
				combined |= uint(value)
				cycle = 3
			case 3:
				combined <<= 6
				combined |= uint(value)
				b[j+2] = byte(combined)
				combined >>= 8
				b[j+1] = byte(combined)
				combined >>= 8
				b[j] = byte(combined)
				j += 3
				cycle = 0
			}
		}
	}

	if cycle != 0 {
		return nil, errors.New("Input to decode not an even multiple of 4 characters; pad with =.")
	}

	j -= dummies
	if j != len(b) {
		b2 := make([]byte, j)
		copy(b2, b)
		b = b2
	}

	return b, nil
}

func init() {
	for i := 0; i < 256; i++ {
		charToValue[i] = int16(IGNORE)
	}

	var index int8
	for i := 'A'; i <= 'Z'; i++ {
		valueToChar[index] = int8(i)
		index++
	}
	for i := 'a'; i <= 'z'; i++ {
		valueToChar[index] = int8(i)
		index++
	}
	for i := '0'; i <= '9'; i++ {
		valueToChar[index] = int8(i)
		index++
	}
	valueToChar[index] = CH_PLUS
	index++
	valueToChar[index] = CH_SPLASH

	for i := 0; i < 64; i++ {
		charToValue[valueToChar[i]] = int16(i)
	}
	charToValue[CH_PAD] = PAD
}
