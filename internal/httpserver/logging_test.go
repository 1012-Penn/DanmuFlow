package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestZapHTTPLoggerRecordsStructuredRequest(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(core)

	router := gin.New()
	router.Use(zapHTTPLogger(logger))
	router.GET("/healthz", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if logs.Len() != 1 {
		t.Fatalf("log count = %d, want 1", logs.Len())
	}

	fields := logs.All()[0].ContextMap()
	if fields["method"] != http.MethodGet || fields["path"] != "/healthz" || fields["status"] != int64(http.StatusOK) {
		t.Fatalf("structured fields = %+v", fields)
	}
	if _, ok := fields["latency"]; !ok {
		t.Fatalf("structured fields = %+v, want latency", fields)
	}
}
