package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

type HTTPClient struct {
	botToken   string
	apiBaseURL string
	httpClient *http.Client
}

type rateLimitError struct {
	cause      error
	retryAfter time.Duration
}

func (e *rateLimitError) Error() string {
	return e.cause.Error()
}

func (e *rateLimitError) Unwrap() error {
	return e.cause
}

func NewRateLimitError(cause error, retryAfter time.Duration) error {
	if cause == nil || retryAfter <= 0 {
		return cause
	}
	return &rateLimitError{cause: cause, retryAfter: retryAfter}
}

func IsRateLimitError(err error) (time.Duration, bool) {
	var target *rateLimitError
	if !errors.As(err, &target) {
		return 0, false
	}
	return target.retryAfter, true
}

func wrapRateLimitError(err error, retry bool, delay time.Duration) error {
	if err == nil || !retry || delay <= 0 {
		return err
	}
	return NewRateLimitError(err, delay)
}

var _ Client = (*HTTPClient)(nil)

func NewHTTPClient(botToken, apiBaseURL string, httpClient *http.Client) *HTTPClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &HTTPClient{
		botToken:   botToken,
		apiBaseURL: strings.TrimRight(apiBaseURL, "/"),
		httpClient: httpClient,
	}
}

func (c *HTTPClient) Upload(ctx context.Context, request UploadRequest) (UploadedFile, error) {
	methodName, fieldName, err := uploadMethodAndField(request.Type)
	if err != nil {
		return UploadedFile{}, err
	}

	requestURL := c.methodURL(methodName)
	resp, err := c.doUploadRequest(ctx, requestURL, request, fieldName)
	if err != nil {
		return UploadedFile{}, err
	}

	var envelope uploadEnvelope
	if err := json.Unmarshal(resp, &envelope); err != nil {
		return UploadedFile{}, fmt.Errorf("decode telegram upload response: %w", err)
	}
	if !envelope.OK {
		return UploadedFile{}, telegramAPIError(envelope.Description)
	}

	file, err := uploadedFileFromResult(request.Type, envelope.Result)
	if err != nil {
		return UploadedFile{}, err
	}
	file.Type = request.Type
	file.MessageID = envelope.Result.MessageID
	return file, nil
}

func (c *HTTPClient) Download(ctx context.Context, fileID string) (io.ReadCloser, error) {
	values := url.Values{}
	values.Set("file_id", fileID)

	resp, err := c.doJSONRequest(ctx, http.MethodPost, c.methodURL("getFile"), "application/x-www-form-urlencoded", func() (io.ReadCloser, string, error) {
		return io.NopCloser(strings.NewReader(values.Encode())), "application/x-www-form-urlencoded", nil
	})
	if err != nil {
		return nil, err
	}

	var envelope getFileEnvelope
	if err := json.Unmarshal(resp, &envelope); err != nil {
		return nil, fmt.Errorf("decode telegram getFile response: %w", err)
	}
	if !envelope.OK {
		return nil, telegramAPIError(envelope.Description)
	}
	if envelope.Result.FilePath == "" {
		return nil, errors.New("telegram getFile response missing file_path")
	}

	return c.downloadFile(ctx, envelope.Result.FilePath)
}

func (c *HTTPClient) downloadFile(ctx context.Context, filePath string) (io.ReadCloser, error) {
	fileURL := c.fileURL(filePath)
	var lastErr error

	for attempt := 0; attempt < 4; attempt++ {
		req, err := c.newRequestWithContext(ctx, http.MethodGet, fileURL, nil, "")
		if err != nil {
			return nil, err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = c.safeRequestError("telegram download failed", err)
		} else {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return resp.Body, nil
			}

			data, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return nil, fmt.Errorf("read telegram download response: %w", readErr)
			}

			retry, delay, parseErr := shouldRetryTelegram(resp.StatusCode, data)
			if parseErr != nil {
				return nil, parseErr
			}
			if !retry {
				return nil, statusError("download_read", resp.StatusCode, data)
			}
			lastErr = statusError("download_read", resp.StatusCode, data)
			if err := sleepWithContext(ctx, backoffDelay(ctx, c.httpClient.Timeout, attempt, delay)); err != nil {
				return nil, err
			}
			continue
		}

		if attempt == 3 {
			break
		}
		if err := sleepWithContext(ctx, backoffDelay(ctx, c.httpClient.Timeout, attempt, 0)); err != nil {
			return nil, err
		}
	}

	if lastErr == nil {
		lastErr = errors.New("telegram download failed")
	}
	return nil, lastErr
}

func (c *HTTPClient) uploadBody(request UploadRequest, fieldName string) (io.ReadCloser, string) {
	pipeReader, pipeWriter := io.Pipe()
	// pipeReader must be closed on every path so the multipart writer goroutine can
	// observe the shutdown and exit instead of blocking on writes to the pipe.
	writer := multipart.NewWriter(pipeWriter)

	go func() {
		defer pipeWriter.Close()
		defer writer.Close()

		if err := writer.WriteField("chat_id", request.ChatID); err != nil {
			_ = pipeWriter.CloseWithError(err)
			return
		}
		if request.Caption != "" {
			if err := writer.WriteField("caption", request.Caption); err != nil {
				_ = pipeWriter.CloseWithError(err)
				return
			}
		}

		part, err := createFilePart(writer, fieldName, request.Filename, request.MIMEType)
		if err != nil {
			_ = pipeWriter.CloseWithError(err)
			return
		}
		if _, err := io.Copy(part, request.Reader); err != nil {
			_ = pipeWriter.CloseWithError(err)
			return
		}
	}()

	return pipeReader, writer.FormDataContentType()
}

func (c *HTTPClient) doUploadRequest(ctx context.Context, requestURL string, request UploadRequest, fieldName string) ([]byte, error) {
	var lastErr error
	var lastDelay time.Duration
	readSeeker, replayable := request.Reader.(io.ReadSeeker)

	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			if !replayable {
				if lastErr != nil {
					return nil, lastErr
				}
				return nil, errors.New("telegram upload cannot retry non-seekable reader")
			}
			if _, err := readSeeker.Seek(0, io.SeekStart); err != nil {
				return nil, fmt.Errorf("reset telegram upload reader: %w", err)
			}
		}

		attemptRequest := request
		attemptRequest.Reader = request.Reader
		if replayable {
			attemptRequest.Reader = readSeeker
		}
		reader, contentType := c.uploadBody(attemptRequest, fieldName)
		data, retry, delay, err := c.doSingleUploadAttempt(ctx, requestURL, reader, contentType)
		if err == nil {
			return data, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !retry {
			return nil, err
		}
		lastErr = err
		lastDelay = delay
		if !replayable {
			return nil, safeUploadReplayError(err)
		}
		if attempt == 3 {
			break
		}
		if err := sleepWithContext(ctx, backoffDelay(ctx, c.httpClient.Timeout, attempt, delay)); err != nil {
			return nil, err
		}
	}

	if lastErr == nil {
		lastErr = errors.New("telegram upload failed")
	}
	return nil, wrapRateLimitError(lastErr, true, lastDelay)
}

func (c *HTTPClient) doSingleUploadAttempt(ctx context.Context, requestURL string, reader io.ReadCloser, contentType string) ([]byte, bool, time.Duration, error) {
	defer reader.Close()

	req, err := c.newRequestWithContext(ctx, http.MethodPost, requestURL, reader, contentType)
	if err != nil {
		return nil, false, 0, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, 0, ctx.Err()
		}
		return nil, true, 0, c.safeRequestError("telegram request failed", err)
	}

	data, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		return nil, false, 0, fmt.Errorf("read telegram response: %w", readErr)
	}

	retry, delay, parseErr := shouldRetryTelegram(resp.StatusCode, data)
	if parseErr != nil {
		return nil, false, 0, parseErr
	}
	if !retry && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return data, false, 0, nil
	}
	return nil, retry, delay, statusError("upload_send", resp.StatusCode, data)
}

func (c *HTTPClient) doJSONRequest(ctx context.Context, method, requestURL string, contentType string, bodyFactory func() (io.ReadCloser, string, error)) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt < 4; attempt++ {
		reader, reqContentType, err := bodyFactory()
		if err != nil {
			return nil, err
		}

		req, err := c.newRequestWithContext(ctx, method, requestURL, reader, firstNonEmpty(reqContentType, contentType))
		if err != nil {
			reader.Close()
			return nil, err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			reader.Close()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = c.safeRequestError("telegram request failed", err)
		} else {
			data, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return nil, fmt.Errorf("read telegram response: %w", readErr)
			}

			retry, delay, parseErr := shouldRetryTelegram(resp.StatusCode, data)
			if parseErr != nil {
				return nil, parseErr
			}
			if !retry && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return data, nil
			}
			if !retry {
				return nil, statusError("request", resp.StatusCode, data)
			}
			lastErr = statusError("request", resp.StatusCode, data)
			if err := sleepWithContext(ctx, backoffDelay(ctx, c.httpClient.Timeout, attempt, delay)); err != nil {
				return nil, err
			}
			continue
		}

		if attempt == 3 {
			break
		}
		if err := sleepWithContext(ctx, backoffDelay(ctx, c.httpClient.Timeout, attempt, 0)); err != nil {
			return nil, err
		}
	}

	if lastErr == nil {
		lastErr = errors.New("telegram request failed")
	}
	return nil, lastErr
}

func (c *HTTPClient) newRequestWithContext(ctx context.Context, method, requestURL string, body io.Reader, contentType string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return nil, c.safeRequestError("create telegram request", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func safeUploadReplayError(cause error) error {
	if cause == nil {
		return errors.New("telegram upload cannot retry non-seekable reader")
	}
	return fmt.Errorf("telegram upload cannot safely retry non-seekable reader after failure: %w", cause)
}

func (c *HTTPClient) safeRequestError(message string, err error) error {
	return fmt.Errorf("%s: %s", message, c.sanitizeErrorText(err.Error()))
}

func (c *HTTPClient) sanitizeErrorText(text string) string {
	if text == "" {
		return text
	}
	if c.botToken != "" {
		text = strings.ReplaceAll(text, c.botToken, "[redacted]")
	}
	text = strings.ReplaceAll(text, "/bot[redacted]/", "/bot<TOKEN>/")
	text = strings.ReplaceAll(text, "/file/bot[redacted]/", "/file/bot<TOKEN>/")
	return text
}

func createFilePart(writer *multipart.Writer, fieldName, filename, mimeType string) (io.Writer, error) {
	header := textproto.MIMEHeader{}
	contentDisposition := fmt.Sprintf(`form-data; name=%q`, fieldName)
	if filename != "" {
		contentDisposition += fmt.Sprintf(`; filename=%q`, filename)
	}
	header.Set("Content-Disposition", contentDisposition)
	if mimeType != "" {
		header.Set("Content-Type", mimeType)
	}
	return writer.CreatePart(header)
}

func uploadMethodAndField(fileType string) (string, string, error) {
	switch fileType {
	case TypePhoto:
		return "sendPhoto", "photo", nil
	case TypeVideo:
		return "sendVideo", "video", nil
	case TypeAudio:
		return "sendAudio", "audio", nil
	case TypeAnimation:
		return "sendAnimation", "animation", nil
	case TypeDocument:
		return "sendDocument", "document", nil
	default:
		return "", "", fmt.Errorf("unsupported telegram upload type %q", fileType)
	}
}

func (c *HTTPClient) methodURL(method string) string {
	return c.apiBaseURL + "/bot" + c.botToken + "/" + method
}

func (c *HTTPClient) fileURL(filePath string) string {
	return c.apiBaseURL + "/file/bot" + c.botToken + "/" + strings.TrimLeft(path.Clean(filePath), "/")
}

func shouldRetryTelegram(statusCode int, data []byte) (bool, time.Duration, error) {
	if statusCode == http.StatusTooManyRequests || statusCode >= 500 {
		var envelope telegramErrorEnvelope
		if len(data) > 0 && json.Unmarshal(data, &envelope) == nil {
			if envelope.Parameters.RetryAfter > 0 {
				return true, time.Duration(envelope.Parameters.RetryAfter) * time.Second, nil
			}
		}
		return true, 0, nil
	}
	return false, 0, nil
}

func backoffDelay(ctx context.Context, clientTimeout time.Duration, attempt int, preferred time.Duration) time.Duration {
	if preferred > 0 {
		return cappedRetryAfter(ctx, clientTimeout, preferred)
	}
	delay := 25 * time.Millisecond * time.Duration(1<<attempt)
	if delay > 200*time.Millisecond {
		return 200 * time.Millisecond
	}
	return delay
}

func cappedRetryAfter(ctx context.Context, clientTimeout, retryAfter time.Duration) time.Duration {
	if retryAfter <= 0 {
		return 0
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0
		}
		if retryAfter > remaining {
			return remaining
		}
		return retryAfter
	}
	if clientTimeout > 0 && retryAfter > clientTimeout {
		return clientTimeout
	}
	return retryAfter
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	if deadline, ok := ctx.Deadline(); ok && delay >= time.Until(deadline) {
		<-ctx.Done()
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
}

func statusError(operation string, statusCode int, data []byte) error {
	description := ""
	if len(data) > 0 {
		var envelope telegramErrorEnvelope
		if json.Unmarshal(data, &envelope) == nil && envelope.Description != "" {
			description = strings.TrimSpace(envelope.Description)
		}
	}
	if description == "" {
		description = "telegram request failed with status " + strconv.Itoa(statusCode)
	}
	return NewRequestError(operation, statusCode, classifyStatusReason(statusCode, description), errors.New(description))
}

func classifyStatusReason(statusCode int, description string) string {
	text := strings.ToLower(strings.TrimSpace(description))
	switch {
	case statusCode == http.StatusUnauthorized || strings.Contains(text, "unauthorized"):
		return "unauthorized"
	case statusCode == http.StatusForbidden && strings.Contains(text, "forbidden"):
		return "forbidden"
	default:
		return "http_" + strconv.Itoa(statusCode)
	}
}

func telegramAPIError(description string) error {
	description = strings.TrimSpace(description)
	if description == "" {
		description = "telegram api error"
	}
	return errors.New(description)
}

func uploadedFileFromResult(fileType string, result uploadResult) (UploadedFile, error) {
	switch fileType {
	case TypePhoto:
		if len(result.Photo) == 0 {
			return UploadedFile{}, errors.New("telegram upload response missing photo")
		}
		largest := result.Photo[0]
		for _, photo := range result.Photo[1:] {
			if photo.FileSize >= largest.FileSize {
				largest = photo
			}
		}
		return UploadedFile{FileID: largest.FileID, FileUniqueID: largest.FileUniqueID, FileSize: largest.FileSize, MIMEType: largest.MIMEType}, nil
	case TypeVideo:
		return mediaToUploadedFile("video", result.Video)
	case TypeAudio:
		return mediaToUploadedFile("audio", result.Audio)
	case TypeAnimation:
		return mediaToUploadedFile("animation", result.Animation)
	case TypeDocument:
		// Telegram 对 .webp 文件会返回 sticker 消息而不是 document（webp 是 Telegram 贴纸格式）。
		// 回退解析 sticker，使 sendDocument 上传的 webp 也能正常使用。
		if result.Document.FileID != "" {
			return mediaToUploadedFile("document", result.Document)
		}
		if result.Sticker.FileID != "" {
			return mediaToUploadedFile("sticker", result.Sticker)
		}
		return UploadedFile{}, errors.New("telegram upload response missing document or sticker")
	default:
		return UploadedFile{}, fmt.Errorf("unsupported telegram upload type %q", fileType)
	}
}

func mediaToUploadedFile(name string, media telegramFile) (UploadedFile, error) {
	if media.FileID == "" {
		return UploadedFile{}, fmt.Errorf("telegram upload response missing %s", name)
	}
	return UploadedFile{
		FileID:       media.FileID,
		FileUniqueID: media.FileUniqueID,
		FileSize:     media.FileSize,
		MIMEType:     media.MIMEType,
	}, nil
}

type uploadEnvelope struct {
	OK          bool         `json:"ok"`
	Description string       `json:"description"`
	Result      uploadResult `json:"result"`
}

type uploadResult struct {
	MessageID int64          `json:"message_id"`
	Document  telegramFile   `json:"document"`
	Sticker   telegramFile   `json:"sticker"`
	Video     telegramFile   `json:"video"`
	Audio     telegramFile   `json:"audio"`
	Animation telegramFile   `json:"animation"`
	Photo     []telegramFile `json:"photo"`
}

type telegramFile struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileSize     int64  `json:"file_size"`
	MIMEType     string `json:"mime_type"`
}

type getFileEnvelope struct {
	OK          bool          `json:"ok"`
	Description string        `json:"description"`
	Result      getFileResult `json:"result"`
}

type getFileResult struct {
	FileID   string `json:"file_id"`
	FilePath string `json:"file_path"`
	FileSize int64  `json:"file_size"`
}

type telegramErrorEnvelope struct {
	Description string `json:"description"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}
