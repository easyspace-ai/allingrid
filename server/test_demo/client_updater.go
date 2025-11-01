package main

import (
	"fmt"
	"log"
	"time"
)

// UpdaterClient 更新客户端
type UpdaterClient struct {
	httpClient *HTTPClient
	tableID    string
	recordID   string
	fieldID    string
}

// NewUpdaterClient 创建更新客户端
func NewUpdaterClient(httpClient *HTTPClient, tableID, recordID, fieldID string) *UpdaterClient {
	return &UpdaterClient{
		httpClient: httpClient,
		tableID:    tableID,
		recordID:   recordID,
		fieldID:    fieldID,
	}
}

// UpdateField 更新字段值
func (c *UpdaterClient) UpdateField(value string) error {
	log.Printf("🔄 更新记录字段: tableID=%s, recordID=%s, fieldID=%s, value=%s\n",
		c.tableID, c.recordID, c.fieldID, value)

	fields := map[string]interface{}{
		c.fieldID: value,
	}

	if err := updateRecord(c.httpClient, c.tableID, c.recordID, fields); err != nil {
		return fmt.Errorf("update record failed: %w", err)
	}

	log.Println("✅ 记录更新成功")
	return nil
}

// UpdateFieldMultiple 多次更新字段值（用于测试）
func (c *UpdaterClient) UpdateFieldMultiple(values []string, interval time.Duration) error {
	for i, value := range values {
		log.Printf("🔄 [%d/%d] 更新字段值为: %s\n", i+1, len(values), value)

		if err := c.UpdateField(value); err != nil {
			return fmt.Errorf("update field failed at iteration %d: %w", i+1, err)
		}

		if i < len(values)-1 {
			log.Printf("⏳ 等待 %v 后继续...\n", interval)
			time.Sleep(interval)
		}
	}

	return nil
}

