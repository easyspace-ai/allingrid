package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// AttachmentSignatureResponse 上传签名响应
type AttachmentSignatureResponse struct {
	Data struct {
		Token        string   `json:"token"`
		UploadURL    string   `json:"upload_url"`
		ExpiresAt    int64    `json:"expires_at"`
		MaxSize      int64    `json:"max_size"`
		AllowedTypes []string `json:"allowed_types"`
	} `json:"data"`
	Message string `json:"message"`
}

// AttachmentUploadResponse 上传响应
type AttachmentUploadResponse struct {
	Data struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	} `json:"data"`
	Message string `json:"message"`
}

// AttachmentNotifyResponse 通知响应
type AttachmentNotifyResponse struct {
	Data struct {
		Attachment struct {
			ID          string `json:"id"`
			Name        string `json:"name"`
			Path        string `json:"path"`
			Token       string `json:"token"`
			Size        int64  `json:"size"`
			MimeType    string `json:"mimetype"`
			PresignedURL string `json:"presigned_url,omitempty"`
		} `json:"attachment"`
		Success bool   `json:"success"`
		Message string `json:"message"`
	} `json:"data"`
	Message string `json:"message"`
}

// AttachmentItem 附件项
type AttachmentItem struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Path        string `json:"path"`
	Token       string `json:"token"`
	Size        int64  `json:"size"`
	MimeType    string `json:"mimetype"`
	PresignedURL string `json:"presigned_url,omitempty"`
}

// AttachmentListResponse 附件列表响应
type AttachmentListResponse struct {
	Data []AttachmentItem `json:"data"`
	Message string `json:"message"`
}

// AttachmentStatsResponse 附件统计响应
type AttachmentStatsResponse struct {
	Data struct {
		TotalFiles    int64     `json:"total_files"`
		TotalSize     int64     `json:"total_size"`
		ImageFiles    int64     `json:"image_files"`
		VideoFiles    int64     `json:"video_files"`
		AudioFiles    int64     `json:"audio_files"`
		DocumentFiles int64     `json:"document_files"`
		OtherFiles    int64     `json:"other_files"`
		LastUploaded  time.Time `json:"last_uploaded"`
	} `json:"data"`
	Message string `json:"message"`
}

// generateAttachmentSignature 生成上传签名
func generateAttachmentSignature(client *HTTPClient, tableID, fieldID, recordID string) (*AttachmentSignatureResponse, error) {
	data := map[string]interface{}{
		"table_id":  tableID,
		"field_id":  fieldID,
		"record_id": recordID,
	}

	resp, err := client.Request("POST", "/api/v1/attachments/signature", data)
	if err != nil {
		return nil, fmt.Errorf("generate signature request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read signature response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("generate signature failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("generate signature failed with status %d: %s", resp.StatusCode, string(body))
	}

	var signatureResp AttachmentSignatureResponse
	if err := json.Unmarshal(body, &signatureResp); err != nil {
		return nil, fmt.Errorf("parse signature response: %w", err)
	}

	return &signatureResp, nil
}

// uploadFile 上传文件
func uploadFile(client *HTTPClient, token string, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open file failed: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("get file info failed: %w", err)
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	part, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return fmt.Errorf("create form file failed: %w", err)
	}

	_, err = io.Copy(part, file)
	if err != nil {
		return fmt.Errorf("copy file content failed: %w", err)
	}

	err = writer.Close()
	if err != nil {
		return fmt.Errorf("close writer failed: %w", err)
	}

	req, err := http.NewRequest("POST", client.BaseURL+fmt.Sprintf("/api/v1/attachments/upload/%s", token), body)
	if err != nil {
		return fmt.Errorf("create upload request failed: %w", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	if client.Token != "" {
		req.Header.Set("Authorization", "Bearer "+client.Token)
	}

	resp, err := client.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("send upload request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read upload response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(respBody, &apiErr); err == nil {
			return fmt.Errorf("upload failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// notifyUpload 通知上传完成
func notifyUpload(client *HTTPClient, token, filename string) (*AttachmentNotifyResponse, error) {
	data := map[string]interface{}{
		"token":    token,
		"filename": filename,
	}

	resp, err := client.Request("POST", fmt.Sprintf("/api/v1/attachments/notify/%s", token), data)
	if err != nil {
		return nil, fmt.Errorf("notify upload request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read notify response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("notify upload failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("notify upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	var notifyResp AttachmentNotifyResponse
	if err := json.Unmarshal(body, &notifyResp); err != nil {
		return nil, fmt.Errorf("parse notify response: %w", err)
	}

	return &notifyResp, nil
}

// getAttachment 获取附件信息
func getAttachment(client *HTTPClient, attachmentID string) (*AttachmentItem, error) {
	resp, err := client.Request("GET", fmt.Sprintf("/api/v1/attachments/%s", attachmentID), nil)
	if err != nil {
		return nil, fmt.Errorf("get attachment request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read get attachment response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("get attachment failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("get attachment failed with status %d: %s", resp.StatusCode, string(body))
	}

	var respData struct {
		Data    AttachmentItem `json:"data"`
		Message string         `json:"message"`
	}
	if err := json.Unmarshal(body, &respData); err != nil {
		return nil, fmt.Errorf("parse get attachment response: %w", err)
	}

	return &respData.Data, nil
}

// listAttachments 列出附件
func listAttachments(client *HTTPClient, tableID, fieldID, recordID string) ([]AttachmentItem, error) {
	path := "/api/v1/attachments"
	var params []string
	if tableID != "" {
		params = append(params, "table_id="+tableID)
	}
	if fieldID != "" {
		params = append(params, "field_id="+fieldID)
	}
	if recordID != "" {
		params = append(params, "record_id="+recordID)
	}
	if len(params) > 0 {
		path += "?" + strings.Join(params, "&")
	}

	resp, err := client.Request("GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("list attachments request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read list attachments response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("list attachments failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("list attachments failed with status %d: %s", resp.StatusCode, string(body))
	}

	var listResp AttachmentListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("parse list attachments response: %w", err)
	}

	return listResp.Data, nil
}

// getAttachmentStats 获取附件统计
func getAttachmentStats(client *HTTPClient, tableID string) (*AttachmentStatsResponse, error) {
	resp, err := client.Request("GET", fmt.Sprintf("/api/v1/tables/%s/attachments/stats", tableID), nil)
	if err != nil {
		return nil, fmt.Errorf("get attachment stats request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read attachment stats response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("get attachment stats failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("get attachment stats failed with status %d: %s", resp.StatusCode, string(body))
	}

	var statsResp AttachmentStatsResponse
	if err := json.Unmarshal(body, &statsResp); err != nil {
		return nil, fmt.Errorf("parse attachment stats response: %w", err)
	}

	return &statsResp, nil
}

// readFile 读取文件
func readFile(client *HTTPClient, path string) ([]byte, error) {
	resp, err := client.Request("GET", fmt.Sprintf("/api/v1/attachments/read/%s", path), nil)
	if err != nil {
		return nil, fmt.Errorf("read file request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read file response failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, fmt.Errorf("read file failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return nil, fmt.Errorf("read file failed with status %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}

// deleteAttachment 删除附件
func deleteAttachment(client *HTTPClient, attachmentID string) error {
	resp, err := client.Request("DELETE", fmt.Sprintf("/api/v1/attachments/%s", attachmentID), nil)
	if err != nil {
		return fmt.Errorf("delete attachment request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read delete response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return fmt.Errorf("delete attachment failed: %s - %s", apiErr.Error, apiErr.Message)
		}
		return fmt.Errorf("delete attachment failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// createTestFile 创建测试文件
func createTestFile(filename string, content string) (string, error) {
	// 创建临时目录
	tmpDir := os.TempDir()
	testDir := filepath.Join(tmpDir, "luckdb_test")
	if err := os.MkdirAll(testDir, 0755); err != nil {
		return "", fmt.Errorf("create test dir failed: %w", err)
	}

	filePath := filepath.Join(testDir, filename)
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		return "", fmt.Errorf("write test file failed: %w", err)
	}

	return filePath, nil
}

// cleanupTestFile 清理测试文件
func cleanupTestFile(filePath string) error {
	return os.Remove(filePath)
}


