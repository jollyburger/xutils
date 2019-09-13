package log

import "go.uber.org/zap"

type LogConfig struct {
	Production bool `json:"is_production"` //default: false
}

var (
	logConfig LogConfig
)

func InitLog(logConf LogConfig) {
	logConfig = logConf
}

func IsProduction() bool {
	return logConfig.Production
}

func GetLogger() (logger *zap.Logger, err error) {
	if logConfig.Production {
		logger, err = zap.NewProduction()
	} else {
		logger, err = zap.NewDevelopment()
	}

	return
}
