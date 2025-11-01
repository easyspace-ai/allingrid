package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// AuthResponse 认证响应
type AuthResponse struct {
	Data struct {
		User struct {
			ID    string `json:"id"`
			Email string `json:"email"`
			Name  string `json:"name"`
		} `json:"user"`
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
	} `json:"data"`
	Message string `json:"message"`
}

// SpaceResponse 空间响应
type SpaceResponse struct {
	Data struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"data"`
	Message string `json:"message"`
}

// BaseResponse Base响应
type BaseResponse struct {
	Data struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		SpaceID string `json:"spaceId"`
	} `json:"data"`
	Message string `json:"message"`
}

// TableResponse 表响应
type TableResponse struct {
	Data struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		BaseID string `json:"baseId"`
	} `json:"data"`
	Message string `json:"message"`
}

// FieldResponse 字段响应
type FieldResponse struct {
	Data struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Type    string `json:"type"`
		TableID string `json:"tableId"`
	} `json:"data"`
	Message string `json:"message"`
}

// RecordResponse 记录响应
type RecordResponse struct {
	Data struct {
		ID      string                 `json:"id"`
		Fields  map[string]interface{} `json:"fields"`
		TableID string                 `json:"tableId"`
	} `json:"data"`
	Message string `json:"message"`
}

// APIError API错误响应
type APIError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// HTTPClient HTTP客户端
type HTTPClient struct {
	BaseURL    string
	Token      string
	HTTPClient *http.Client
}

// NewHTTPClient 创建HTTP客户端
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// SetToken 设置Token
func (c *HTTPClient) SetToken(token string) {
	c.Token = token
}

// Request 发送HTTP请求
func (c *HTTPClient) Request(method, path string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequest(method, c.BaseURL+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	return resp, nil
}

// registerAndLogin 注册并登录测试账号
func registerAndLogin(client *HTTPClient, email, password, name string) (*AuthResponse, error) {
	// 先尝试注册
	registerData := map[string]interface{}{
		"email":    email,
		"password": password,
		"name":     name,
	}

	resp, err := client.Request("POST", "/api/v1/auth/register", registerData)
	if err != nil {
		return nil, fmt.Errorf("register request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read register response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		// 如果注册失败（可能是用户已存在），尝试登录
		if resp.StatusCode == http.StatusConflict {
			fmt.Printf("用户已存在，尝试登录...\n")
			return login(client, email, password)
		}
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("register failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("register failed with status %d: %s", resp.StatusCode, string(body))
	}

	var authResp AuthResponse
	if err := json.Unmarshal(body, &authResp); err != nil {
		return nil, fmt.Errorf("parse register response: %w", err)
	}

	return &authResp, nil
}

// login 登录
func login(client *HTTPClient, email, password string) (*AuthResponse, error) {
	loginData := map[string]interface{}{
		"email":    email,
		"password": password,
	}

	resp, err := client.Request("POST", "/api/v1/auth/login", loginData)
	if err != nil {
		return nil, fmt.Errorf("login request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read login response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("login failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("login failed with status %d: %s", resp.StatusCode, string(body))
	}

	var authResp AuthResponse
	if err := json.Unmarshal(body, &authResp); err != nil {
		return nil, fmt.Errorf("parse login response: %w", err)
	}

	return &authResp, nil
}

// createSpace 创建Space
func createSpace(client *HTTPClient, name string) (*SpaceResponse, error) {
	data := map[string]interface{}{
		"name": name,
	}

	resp, err := client.Request("POST", "/api/v1/spaces", data)
	if err != nil {
		return nil, fmt.Errorf("create space request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read create space response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("create space failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("create space failed with status %d: %s", resp.StatusCode, string(body))
	}

	var spaceResp SpaceResponse
	if err := json.Unmarshal(body, &spaceResp); err != nil {
		return nil, fmt.Errorf("parse create space response: %w", err)
	}

	return &spaceResp, nil
}

// createBase 创建Base
func createBase(client *HTTPClient, spaceID, name string) (*BaseResponse, error) {
	data := map[string]interface{}{
		"name": name,
	}

	resp, err := client.Request("POST", fmt.Sprintf("/api/v1/spaces/%s/bases", spaceID), data)
	if err != nil {
		return nil, fmt.Errorf("create base request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read create base response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("create base failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("create base failed with status %d: %s", resp.StatusCode, string(body))
	}

	var baseResp BaseResponse
	if err := json.Unmarshal(body, &baseResp); err != nil {
		return nil, fmt.Errorf("parse create base response: %w", err)
	}

	return &baseResp, nil
}

// createTable 创建Table
func createTable(client *HTTPClient, baseID, name string) (*TableResponse, error) {
	data := map[string]interface{}{
		"name":   name,
		"baseId": baseID,
	}

	resp, err := client.Request("POST", fmt.Sprintf("/api/v1/bases/%s/tables", baseID), data)
	if err != nil {
		return nil, fmt.Errorf("create table request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read create table response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("create table failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("create table failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tableResp TableResponse
	if err := json.Unmarshal(body, &tableResp); err != nil {
		return nil, fmt.Errorf("parse create table response: %w", err)
	}

	return &tableResp, nil
}

// createField 创建Field
func createField(client *HTTPClient, tableID, name, fieldType string) (*FieldResponse, error) {
	data := map[string]interface{}{
		"name":    name,
		"type":    fieldType,
		"tableId": tableID,
	}

	resp, err := client.Request("POST", fmt.Sprintf("/api/v1/tables/%s/fields", tableID), data)
	if err != nil {
		return nil, fmt.Errorf("create field request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read create field response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("create field failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("create field failed with status %d: %s", resp.StatusCode, string(body))
	}

	var fieldResp FieldResponse
	if err := json.Unmarshal(body, &fieldResp); err != nil {
		return nil, fmt.Errorf("parse create field response: %w", err)
	}

	return &fieldResp, nil
}

// createRecord 创建Record
func createRecord(client *HTTPClient, tableID string, fields map[string]interface{}) (*RecordResponse, error) {
	data := map[string]interface{}{
		"tableId": tableID,
		"data":    fields,
	}

	resp, err := client.Request("POST", fmt.Sprintf("/api/v1/tables/%s/records", tableID), data)
	if err != nil {
		return nil, fmt.Errorf("create record request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read create record response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("create record failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("create record failed with status %d: %s", resp.StatusCode, string(body))
	}

	var recordResp RecordResponse
	if err := json.Unmarshal(body, &recordResp); err != nil {
		return nil, fmt.Errorf("parse create record response: %w", err)
	}

	return &recordResp, nil
}

// updateRecord 更新Record
func updateRecord(client *HTTPClient, tableID, recordID string, fields map[string]interface{}) error {
	data := map[string]interface{}{
		"data": fields,
	}

	resp, err := client.Request("PATCH", fmt.Sprintf("/api/v1/tables/%s/records/%s", tableID, recordID), data)
	if err != nil {
		return fmt.Errorf("update record request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read update record response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return fmt.Errorf("update record failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return fmt.Errorf("update record failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

