package main

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// PacedTransport 为所有 SDK REST 请求提供统一间隔和交易所限流退避。
type PacedTransport struct {
	mu       sync.Mutex
	Base     http.RoundTripper
	Next     time.Time
	Interval time.Duration
}

// RoundTrip 串行发送请求并在 HTTP 限流或服务器错误后延长退避时间。
func (p *PacedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// step.1 在统一发送闸门中等待，不让不同业务绕过请求间隔。
	p.mu.Lock()
	defer p.mu.Unlock()
	if delay := time.Until(p.Next); delay > 0 {
		// 调用本目录辅助函数，让限速等待能够响应请求取消。
		if err := help_wait(req.Context(), delay); err != nil {
			return nil, err
		}
	}
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	p.Next = time.Now().Add(p.Interval)
	resp, err := p.Base.RoundTrip(req)
	// step.2 尊重 Retry-After；没有明确时间时对限流和故障保守退避。
	if err == nil && (resp.StatusCode == 429 || resp.StatusCode == 418 || resp.StatusCode >= 500) {
		delay := 5 * time.Second
		if resp.StatusCode == 429 {
			delay = time.Minute
		}
		if resp.StatusCode == 418 {
			delay = 5 * time.Minute
		}
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		p.Next = time.Now().Add(delay)
	}
	return resp, err
}
