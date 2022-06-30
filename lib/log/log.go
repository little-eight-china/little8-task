package log

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	OsFileName      = "LITTLE8_LOG_FILE_PATH"
	DefaultFilePATH = "./tmp/log/console.log"
)

var Logger LoggerConfig

type LoggerConfig struct {
	once   sync.Once
	logger *zap.Logger
}

func (log *LoggerConfig) Get() *zap.Logger {
	log.once.Do(log.init)
	return log.logger
}

func (log *LoggerConfig) init() {
	log.logger, _ = zap.NewDevelopment()
}

// 获取编码器
func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

// 日志保存位置
func getWriteSyncer() zapcore.WriteSyncer {
	path, ok := os.LookupEnv(OsFileName)
	if !ok {
		path = DefaultFilePATH
	}
	fp, _ := os.Create(path)
	return zapcore.AddSync(fp)
}

func newLogger() *zap.Logger {
	// 日志编码方式
	encoder := getEncoder()
	// 日志保存位置
	writeSyncer := getWriteSyncer()
	// 日志记录等级
	logLevel := zapcore.InfoLevel
	core := zapcore.NewCore(encoder, writeSyncer, logLevel)
	return zap.New(core, zap.AddCaller())
}
