package go_fastdfs

import "bytes"

func disAddr(addr []byte) []byte {
	index := bytes.IndexByte(addr, 0)
	return addr[:index]
}