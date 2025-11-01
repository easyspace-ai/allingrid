package main

import (
	"os"
	"time"
)

// Config 测试配置
type Config struct {
	ServerURL      string
	WebSocketURL   string
	TestEmail      string
	TestPassword   string
	TestName       string
	Timeout        time.Duration
	UpdateInterval time.Duration
}

// LoadConfig 加载配置
func LoadConfig() *Config {
	serverURL := os.Getenv("SERVER_URL")
	if serverURL == "" {
		serverURL = "http://localhost:8080"
	}

	wsURL := os.Getenv("WEBSOCKET_URL")
	if wsURL == "" {
		wsURL = "ws://localhost:8080"
	}

	testEmail := os.Getenv("TEST_EMAIL")
	if testEmail == "" {
		testEmail = "test_demo@example.com"
	}

	testPassword := os.Getenv("TEST_PASSWORD")
	if testPassword == "" {
		testPassword = "Test123456!"
	}

	testName := os.Getenv("TEST_NAME")
	if testName == "" {
		testName = "测试用户"
	}

	return &Config{
		ServerURL:      serverURL,
		WebSocketURL:   wsURL,
		TestEmail:      testEmail,
		TestPassword:   testPassword,
		TestName:       testName,
		Timeout:        30 * time.Second,
		UpdateInterval: 2 * time.Second,
	}
}

