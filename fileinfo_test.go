package go_fastdfs

import (
	"testing"
	"time"
)

func Test_fileinfo(t *testing.T) {
	client := NewClient(&Options{
		Addr:               "11.11.0.72:22122",
		DialTimeout:        10 * time.Second,
		PoolSize:           10,
		PoolTimeout:        20 * time.Second,
		IdleTimeout:        2 * time.Minute,
		IdleCheckFrequency: 60 * time.Second,
	})

	//fileid := "group2/M00/06/33/CwsAhloNOzeAOGe1GJpRX2yJAdc69.epub"
	fileid := "group1/M00/C0/3B/CwsASlZdXWKAFMIiAABet4qJ5yc108.jpg"

	fileinfo, err := client.FileInfo(fileid)
	if err != nil {
		t.Error(err)
	}
	res, err := client.DownloadToIoReader(fileid, 0, fileinfo.FileSize)
	if err != nil {
		t.Error(res)
	}

	//此处必须显示的关闭res资源，否则会导致连接池泄漏
	defer res.Close()

	temp := make([]byte, 1024)
	_, err = res.Read(temp)
	if err != nil {
		t.Error(err)
	}

	t.Log(len(temp))
}
