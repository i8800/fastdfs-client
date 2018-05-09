package go_fastdfs

import "bytes"

//从buff中读取byte不为0的部分
func readStr(buff []byte) string {
	index := bytes.IndexByte(buff, 0)
	return string(buff[:index])
}
