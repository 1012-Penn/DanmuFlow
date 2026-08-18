// Package logging 提供 DanmuFlow 统一的结构化日志初始化入口。
package logging

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const defaultLogLevel = "info"

// New 创建服务级结构化 logger。
// 日志默认以 JSON 输出到 stdout，便于 Docker、Kubernetes 或日志采集器统一收集。
// 可以通过 DANMUFLOW_LOG_LEVEL 设置 debug、info、warn 或 error 等级。
func New() (*zap.Logger, error) {
	levelText := strings.TrimSpace(os.Getenv("DANMUFLOW_LOG_LEVEL"))
	if levelText == "" {
		levelText = defaultLogLevel
	}

	var level zapcore.Level
	if err := level.UnmarshalText([]byte(strings.ToLower(levelText))); err != nil {
		return nil, fmt.Errorf("invalid DANMUFLOW_LOG_LEVEL %q: %w", levelText, err)
	}

	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(level)
	config.OutputPaths = []string{"stdout"}
	config.ErrorOutputPaths = []string{"stderr"}
	// 关键生命周期和拒绝事件需要完整保留；高频正常弹幕不会在业务层逐条记录。
	config.Sampling = nil

	return config.Build()
}
