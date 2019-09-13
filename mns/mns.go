package mns

import ali_mns "github.com/aliyun/aliyun-mns-go-sdk"

type MnsConfig struct {
	Url          string `json:"url"`
	AccessKeyId  string `json:"access_key"`
	AccessSecret string `json:"access_secret"`
	Name         string `json:"name"`
}

var (
	mnsConfig MnsConfig
	mnsClient ali_mns.MNSClient
)

func InitMns(conf MnsConfig) {
	mnsConfig = conf

	if mnsConfig.Url == "" {
		panic("alicloud mns entrypoint error")
	}

	if mnsConfig.AccessKeyId == "" || mnsConfig.AccessSecret == "" {
		panic("alicloud mns authentication error")
	}
}

func GetMnsInstance() ali_mns.MNSClient {
	if mnsClient != nil {
		return mnsClient
	}

	mnsClient = ali_mns.NewAliMNSClient(mnsConfig.Url, mnsConfig.AccessKeyId, mnsConfig.AccessSecret)
	return mnsClient
}

func GetMnsQueue() ali_mns.AliMNSQueue {
	if mnsClient == nil {
		mnsClient = ali_mns.NewAliMNSClient(mnsConfig.Url, mnsConfig.AccessKeyId, mnsConfig.AccessSecret)
	}

	queue := ali_mns.NewMNSQueue(mnsConfig.Name, mnsClient)

	return queue
}
