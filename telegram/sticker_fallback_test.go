package telegram

import "testing"

func TestUploadedFileFromResultFallsBackToStickerForWebP(t *testing.T) {
	// Telegram 对 sendDocument 上传的 .webp 文件返回 sticker 消息而不是 document。
	// TypeDocument 分支必须回退解析 sticker，否则 webp 上传会失败。
	file, err := uploadedFileFromResult(TypeDocument, uploadResult{
		Document: telegramFile{},
		Sticker: telegramFile{
			FileID:       "sticker-file-id-123",
			FileUniqueID: "sticker-unique-123",
			FileSize:     4096,
			MIMEType:     "image/webp",
		},
	})
	if err != nil {
		t.Fatalf("expected sticker fallback to succeed, got error: %v", err)
	}
	if file.FileID != "sticker-file-id-123" {
		t.Fatalf("FileID = %q, want %q", file.FileID, "sticker-file-id-123")
	}
	if file.FileSize != 4096 {
		t.Fatalf("FileSize = %d, want 4096", file.FileSize)
	}
}

func TestUploadedFileFromResultDocumentStillWorks(t *testing.T) {
	// 普通 document 响应不受影响。
	file, err := uploadedFileFromResult(TypeDocument, uploadResult{
		Document: telegramFile{
			FileID:       "doc-file-id-456",
			FileUniqueID: "doc-unique-456",
			FileSize:     1024,
			MIMEType:     "application/pdf",
		},
	})
	if err != nil {
		t.Fatalf("expected document parse to succeed, got error: %v", err)
	}
	if file.FileID != "doc-file-id-456" {
		t.Fatalf("FileID = %q, want %q", file.FileID, "doc-file-id-456")
	}
}

func TestUploadedFileFromResultMissingBothReturnsError(t *testing.T) {
	// document 和 sticker 都缺失时报错。
	_, err := uploadedFileFromResult(TypeDocument, uploadResult{})
	if err == nil {
		t.Fatal("expected error when both document and sticker are missing")
	}
}
