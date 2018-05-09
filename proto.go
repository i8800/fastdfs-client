package go_fastdfs

import (
	"bytes"
	"strconv"
	"strings"
	"errors"
)

const (
	TRACKER_PROTO_CMD_RESP                                  = 100
	TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE = 101
	TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE               = 102
	TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE = 103

	STORAGE_PROTO_CMD_QUERY_FILE_INFO = 22
	STORAGE_PROTO_CMD_RESP            = TRACKER_PROTO_CMD_RESP
	STORAGE_PROTO_CMD_DOWNLOAD_FILE = 14

	FDFS_PROTO_CMD_ACTIVE_TEST = 111

	FDFS_GROUP_NAME_MAX_LEN = 16
	FDFS_IPADDR_SIZE        = 16
	FDFS_PROTO_PKG_LEN_SIZE = 8

	PROTO_HEADER_CMD_INDEX    = FDFS_PROTO_PKG_LEN_SIZE
	PROTO_HEADER_STATUS_INDEX = FDFS_PROTO_PKG_LEN_SIZE + 1

	FDFS_LOGIC_FILE_PATH_LEN    = 10
	FDFS_FILENAME_BASE64_LENGTH = 27
	FDFS_FILE_EXT_NAME_MAX_LEN  = 6
	FDFS_TRUNK_FILE_INFO_LEN    = 16

	TRUNK_FILE_MARK_SIZE         = 512 << 50
	APPENDER_FILE_SIZE           = TRUNK_FILE_MARK_SIZE
	NORMAL_LOGIC_FILENAME_LENGTH = FDFS_LOGIC_FILE_PATH_LEN + FDFS_FILENAME_BASE64_LENGTH + FDFS_FILE_EXT_NAME_MAX_LEN + 1
	TRUNK_LOGIC_FILENAME_LENGTH  = NORMAL_LOGIC_FILENAME_LENGTH + FDFS_TRUNK_FILE_INFO_LEN

	TRACKER_QUERY_STORAGE_STORE_BODY_LEN = (FDFS_GROUP_NAME_MAX_LEN + FDFS_IPADDR_SIZE - 1 + FDFS_PROTO_PKG_LEN_SIZE + 1)
	TRACKER_QUERY_STORAGE_FETCH_BODY_LEN = (FDFS_GROUP_NAME_MAX_LEN + FDFS_IPADDR_SIZE - 1 + FDFS_PROTO_PKG_LEN_SIZE)
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

func Int64ToBuff(n int64) []byte {
	b := make([]byte, 8)
	b[0] = byte((n >> 56) & 0xFF)
	b[1] = byte((n >> 48) & 0xFF)
	b[2] = byte((n >> 40) & 0xFF)
	b[3] = byte((n >> 32) & 0xFF)
	b[4] = byte((n >> 24) & 0xFF)
	b[5] = byte((n >> 16) & 0xFF)
	b[6] = byte((n >> 8) & 0xFF)
	b[7] = byte(n & 0xFF)

	return b
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

//组装groupname
func buildGroupName(gn string) []byte {
	b := make([]byte, FDFS_GROUP_NAME_MAX_LEN)
	copy(b, []byte(gn))
	return b
}


func splitFileid(fid string) (string, string, error) {
	fid = strings.TrimSpace(fid)
	p := strings.SplitN(fid, "/", 2)
	if len(p) < 2 {
		return "", "", errors.New("Fileid format error.")
	}

	return p[0], p[1], nil
}