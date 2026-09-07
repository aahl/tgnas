package telegram

import "testing"

func TestUploadedFileFromResultFallsBackToStickerForWebP(t *testing.T) {
	// Telegram returns a sticker message instead of a document for .webp files
	// uploaded via sendDocument. The TypeDocument branch must fall back to
	// parsing the sticker, otherwise .webp uploads fail.
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
	// Regular document responses are unaffected.
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
	// An error is returned when both document and sticker are missing.
	_, err := uploadedFileFromResult(TypeDocument, uploadResult{})
	if err == nil {
		t.Fatal("expected error when both document and sticker are missing")
	}
}
