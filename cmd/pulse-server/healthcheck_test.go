package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCheckHealthOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			t.Errorf("path = %q, want /readyz", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := checkHealth(srv.URL, time.Second); err != nil {
		t.Fatalf("checkHealth() error = %v, want nil", err)
	}
}

func TestCheckHealthNonOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	if err := checkHealth(srv.URL, time.Second); err == nil {
		t.Fatal("checkHealth() error = nil, want an error for non-200 status")
	}
}

func TestHealthBaseURL(t *testing.T) {
	cases := map[string]string{
		"0.0.0.0:9091":     "http://127.0.0.1:9091",
		"127.0.0.1:9091":   "http://127.0.0.1:9091",
		"monitor.local:80": "http://monitor.local:80",
	}
	for addr, want := range cases {
		if got := healthBaseURL(addr); got != want {
			t.Errorf("healthBaseURL(%q) = %q, want %q", addr, got, want)
		}
	}
}
