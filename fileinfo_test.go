package go_fastdfs

import (
	"fmt"
	"testing"
	"time"
)

func Test_fileinfo(t *testing.T) {
	client := NewClient(&Options{
		Addr:               "11.11.1.72:22122",
		DialTimeout:        10 * time.Second,
		PoolSize:           10,
		PoolTimeout:        20 * time.Second,
		IdleTimeout:        2 * time.Minute,
		IdleCheckFrequency: 60 * time.Second,
	})

	res, err := client.getStorageInfo("group2/M00/06/33/CwsAhloNOzeAOGe1GJpRX2yJAdc69.epub")
	if err != nil {
		panic(err)
	}

	fmt.Println(res)
}
