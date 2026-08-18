package bus

import (
	"context"
	"testing"

	"github.com/1012-Penn/DanmuFlow/internal/message"
)

type compileOnlyBus struct{}

func (compileOnlyBus) Publish(context.Context, message.Danmaku) error {
	return nil
}

func (compileOnlyBus) Consume(context.Context, Handler) error {
	return nil
}

func TestCompileOnlyBusImplementsContract(t *testing.T) {
	var _ Bus = compileOnlyBus{}
}
