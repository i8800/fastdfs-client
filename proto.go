package go_fastdfs

import (
	"bytes"
	"strconv"
)

const (
	TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE = 101 //上传
	TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE               = 102 //下载
	STORAGE_PROTO_CMD_QUERY_FILE_INFO                       = 22  //获取文件信息

	FDFS_GROUP_NAME_MAX_LEN = 16 //groupname 16byte
	IP_ADDRESS_SIZE         = 16 //ip
	FDFS_PROTO_PKG_LEN_SIZE = 8  //port

	FDFS_LOGIC_FILE_PATH_LEN    = 10
	FDFS_FILENAME_BASE64_LENGTH = 27

	TRACKER_QUERY_STORAGE_STORE_BODY_LEN = (FDFS_GROUP_NAME_MAX_LEN + IP_ADDRESS_SIZE - 1 + FDFS_PROTO_PKG_LEN_SIZE + 1)
	TRACKER_QUERY_STORAGE_FETCH_BODY_LEN = (FDFS_GROUP_NAME_MAX_LEN + IP_ADDRESS_SIZE - 1 + FDFS_PROTO_PKG_LEN_SIZE)
)

func buffToInt64(bs []byte, offset int) int64 {
	return (int64(bs[0+offset]) << 56) |
		(int64(bs[1+offset]) << 48) |
		(int64(bs[2+offset]) << 40) |
		(int64(bs[3+offset]) << 32) |
		(int64(bs[4+offset]) << 24) |
		(int64(bs[5+offset]) << 16) |
		(int64(bs[6+offset]) << 8) |
		(int64(bs[7+offset]))
}

func buffToInt32(bs []byte, offset int) int32 {
	return (int32(bs[0+offset]) << 24) |
		(int32(bs[1+offset]) << 16) |
		(int32(bs[2+offset]) << 8) |
		(int32(bs[3+offset]))
}

func ipToString(ipInt int) string {
	ipSegs := make([]string, 4)
	var len int = len(ipSegs)
	buffer := bytes.NewBufferString("")
	for i := 0; i < len; i++ {
		tempInt := ipInt & 0xFF
		ipSegs[len-i-1] = strconv.Itoa(tempInt)
		ipInt = ipInt >> 8
	}
	for i := 0; i < len; i++ {
		buffer.WriteString(ipSegs[i])
		if i < len-1 {
			buffer.WriteString(".")
		}
	}
	return buffer.String()
}
