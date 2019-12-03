package oss

import "github.com/aliyun/aliyun-oss-go-sdk/oss"

type OssConfig struct {
	Key        string `json:"key"`
	Secret     string `json:"secret"`
	EndPoint   string `json:"endpoint"`
	Bucket     string `json:"bucket"`
	RetryTimes int    `json:"retry_times"`
	ColdStart  int    `json:"cold_start"`
	Prefix     string `json:"prefix"`
	Proxy      string `json:"proxy"`
}

var (
	ossConfig OssConfig
	ossClient *oss.Client
)

func InitOss(conf OssConfig) {
	ossConfig = conf

	if ossConfig.Key == "" {
		panic("oss key empty")
	}

	if ossConfig.Secret == "" {
		panic("oss secret empty")
	}

	if ossConfig.EndPoint == "" {
		panic("oss endpoint empty")
	}

	if ossConfig.Bucket == "" {
		panic("oss bucket empty")
	}

	if ossConfig.RetryTimes == 0 {
		panic("oss retry times empty")
	}

	if ossConfig.ColdStart == 0 {
		panic("oss cold start empty")
	}
}

func GetOssConfig() OssConfig {
	return ossConfig
}

// oss client
func GetSharedOssClient(bucketName string, endPoint string) (bucket *oss.Bucket, err error) {
	if ossClient == nil {
		ossClient, err = oss.New(endPoint, ossConfig.Key, ossConfig.Secret, oss.Timeout(10, 10))
		if err != nil {
			return nil, err
		}
	}

	bucket, err = ossClient.Bucket(bucketName)
	return bucket, err
}
