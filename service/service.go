package service

import (
	"fmt"

	"github.com/jollyburger/xframe/server"
)

type ServiceConfig struct {
	HttpAddr string `json:"http_addr"`
	HttpPort int    `json:"http_port"`
	TcpAddr  string `json:"tcp_addr"`
	TcpPort  int    `json:"tcp_port"`
}

var (
	serviceConfig ServiceConfig
)

func InitService(conf ServiceConfig) {
	serviceConfig = conf

	if serviceConfig.HttpAddr == "" {
		panic("service http address error")
	}

	if serviceConfig.HttpPort == 0 {
		panic("service http port error")
	}

	if serviceConfig.TcpAddr == "" {
		panic("service tcp address error")
	}

	if serviceConfig.TcpPort == 0 {
		panic("service tcp port error")
	}
}

func GetHttpEntry() string {
	ip, _ := server.ParseListenAddr(serviceConfig.HttpAddr)

	return fmt.Sprintf("%s:%d", ip, serviceConfig.HttpPort)
}

func GetTcpEntry() string {
	ip, _ := server.ParseListenAddr(serviceConfig.TcpAddr)

	return fmt.Sprintf("%s:%d", ip, serviceConfig.TcpPort)
}
