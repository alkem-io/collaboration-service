package main

import (
	"net/http"
	"time"

	httpAdapter "github.com/alkem-io/collaboration-service/internal/adapter/inbound/http"
)

func newPrivateHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       httpAdapter.MemoReadTimeout,
		WriteTimeout:      httpAdapter.MemoReadTimeout + time.Second,
		IdleTimeout:       10 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}
}
