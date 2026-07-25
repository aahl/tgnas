package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aahl/tgnas/metadata"
	"github.com/aahl/tgnas/telegram"
)

type ObjectStore struct {
	meta               metadata.Store
	tg                 telegram.Client
	options            Options
	resolver           *UploadStrategyResolver
	locker             *KeyedLocker
	uploads            chan struct{}
	downloads          chan struct{}
	telegramSem        chan struct{}
	logger             *log.Logger
	startupBuckets     map[string]metadata.Bucket
	bucketBindings     map[string]BucketBinding
	uploadGates        map[string]*uploadGate
	multipartMu        sync.Mutex
	multipartUploads   map[string]*multipartUpload
	multipartSeq       uint64
	unavailableMu      sync.RWMutex
	unavailableBuckets map[string]bool
}

type uploadRecord struct {
	FileID    string
	MessageID int64
}

type uploadGate struct {
	slot  chan struct{}
	mu    sync.Mutex
	until time.Time
}

type stagedUpload struct {
	reader io.ReadSeeker
	close  func() error
	remove func() error
}

const maxMultipartUploads = 1000

type multipartUpload struct {
	bucket      string
	key         string
	contentType string
	createdAt   time.Time
	seq         uint64
	parts       map[int]multipartPart
}

type multipartPart struct {
	number   int
	etag     string
	md5Bytes []byte
	size     int64
	chunks   []multipartChunk
}

type multipartChunk struct {
	size                 int64
	telegramType         string
	telegramFileID       string
	telegramMessageID    int64
	telegramFileUniqueID string
	sha256               string
}

func newUploadGate() *uploadGate {
	gate := &uploadGate{slot: make(chan struct{}, 1)}
	gate.slot <- struct{}{}
	return gate
}

func NewObjectStore(meta metadata.Store, tg telegram.Client, options Options) (*ObjectStore, error) {
	upload := options.Upload
	if upload.Strategy == "" && upload.MaxFileSize == 0 && upload.ChunkSize == 0 && upload.TypeLimits == nil && upload.PutBufferSize == 0 {
		upload = DefaultUploadConfig()
	} else {
		defaults := DefaultUploadConfig()
		if upload.Strategy == "" {
			upload.Strategy = defaults.Strategy
		}
		if upload.MaxFileSize == 0 {
			upload.MaxFileSize = defaults.MaxFileSize
		}
		if upload.ChunkSize == 0 {
			upload.ChunkSize = defaults.ChunkSize
		}
		if upload.TypeLimits == nil {
			upload.TypeLimits = defaults.TypeLimits
		}
		if upload.PutBufferSize == 0 {
			upload.PutBufferSize = defaults.PutBufferSize
		}
	}
	options.Upload = upload

	store := &ObjectStore{
		meta:               meta,
		tg:                 tg,
		options:            options,
		resolver:           NewUploadStrategyResolver(upload),
		locker:             NewKeyedLocker(),
		logger:             options.Logger,
		startupBuckets:     map[string]metadata.Bucket{},
		bucketBindings:     map[string]BucketBinding{},
		uploadGates:        map[string]*uploadGate{},
		multipartUploads:   map[string]*multipartUpload{},
		unavailableBuckets: map[string]bool{},
	}
	if store.logger == nil {
		store.logger = log.New(io.Discard, "", 0)
	}
	for name, binding := range options.Buckets {
		store.bucketBindings[name] = binding
		if _, ok := store.uploadGates[binding.TokenKey]; !ok {
			store.uploadGates[binding.TokenKey] = newUploadGate()
		}
	}
	if tg != nil {
		if _, ok := store.uploadGates[""]; !ok {
			store.uploadGates[""] = newUploadGate()
		}
	}
	if options.MaxUploads > 0 {
		store.uploads = make(chan struct{}, options.MaxUploads)
	}
	if options.MaxDownloads > 0 {
		store.downloads = make(chan struct{}, options.MaxDownloads)
	}
	if options.MaxTelegramCalls > 0 {
		store.telegramSem = make(chan struct{}, options.MaxTelegramCalls)
	}
	if buckets, err := meta.ListBuckets(context.Background()); err == nil {
		for _, bucket := range buckets {
			if !bucket.Enabled {
				continue
			}
			if tg == nil {
				if _, ok := options.Buckets[bucket.Name]; !ok {
					return nil, fmt.Errorf("missing runtime bucket binding for %q", bucket.Name)
				}
			}
			store.startupBuckets[bucket.Name] = bucket
		}
	} else {
		return nil, fmt.Errorf("startup bucket snapshot: %s", sanitizeLogError(err))
	}
	return store, nil
}

func (s *ObjectStore) ListBuckets(ctx context.Context) ([]metadata.Bucket, error) {
	return s.meta.ListBuckets(ctx)
}

func (s *ObjectStore) HeadBucket(ctx context.Context, name string) error {
	if _, ok := s.startupBuckets[name]; !ok {
		return ErrNoSuchBucket
	}
	if s.isBucketUnavailable(name) {
		return ErrUnavailable
	}
	return nil
}

func (s *ObjectStore) isBucketUnavailable(name string) bool {
	s.unavailableMu.RLock()
	defer s.unavailableMu.RUnlock()
	return s.unavailableBuckets[name]
}

func (s *ObjectStore) markBucketUnavailable(name string) {
	s.unavailableMu.Lock()
	defer s.unavailableMu.Unlock()
	s.unavailableBuckets[name] = true
}

func shouldMarkBucketUnavailable(err error) bool {
	requestErr, ok := telegram.ClassifyRequestError(err)
	if !ok {
		return false
	}
	return requestErr.Operation == "upload_send" && requestErr.Reason == "unauthorized"
}

func (s *ObjectStore) DeleteBucket(ctx context.Context, name string) error {
	bucket, err := s.meta.GetBucket(ctx, name)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return ErrNoSuchBucket
		}
		return err
	}
	if bucket.Enabled {
		return ErrNotImplemented
	}
	return s.meta.DeleteBucket(ctx, name)
}

func (s *ObjectStore) PutObject(ctx context.Context, input PutObjectInput) (PutObjectResult, error) {
	if input.Size < 0 {
		return PutObjectResult{}, ErrMissingContentLength
	}
	if err := s.HeadBucket(ctx, input.Bucket); err != nil {
		return PutObjectResult{}, err
	}
	if input.Body == nil {
		input.Body = strings.NewReader("")
	}

	releaseUpload := s.acquire(ctx, s.uploads)
	if releaseUpload == nil {
		return PutObjectResult{}, ctx.Err()
	}
	defer releaseUpload()

	releaseLock := s.locker.Lock(input.Bucket, input.Key)
	defer releaseLock()

	strategy, err := s.resolver.Resolve(path.Base(input.Key), input.ContentType, input.Size)
	if err != nil {
		return PutObjectResult{}, err
	}
	chunkSize := int64(0)
	chunkCount := 1
	if strategy.Chunked {
		chunkSize = strategy.ChunkSize
		if chunkSize <= 0 {
			chunkSize = s.options.Upload.ChunkSize
		}
		chunkCount = int((input.Size + chunkSize - 1) / chunkSize)
	}
	s.logger.Printf("debug event=put_object_decision bucket=%q key=%q size=%d telegram_type=%q strategy=%q chunked=%t chunk_size=%d chunk_count=%d", input.Bucket, input.Key, input.Size, strategy.TelegramType, strategy.UploadStrategy, strategy.Chunked, chunkSize, chunkCount)

	if input.Size == 0 {
		return s.putEmpty(ctx, input, strategy)
	}
	if !strategy.Chunked {
		limit := s.options.Upload.TypeLimits[strategy.TelegramType]
		if limit > 0 && input.Size > limit {
			return PutObjectResult{}, ErrEntityTooLarge
		}
		return s.putSingle(ctx, input, strategy)
	}
	return s.putChunked(ctx, input, strategy)
}

func (s *ObjectStore) CreateMultipartUpload(ctx context.Context, input CreateMultipartUploadInput) (CreateMultipartUploadResult, error) {
	if err := s.HeadBucket(ctx, input.Bucket); err != nil {
		return CreateMultipartUploadResult{}, err
	}
	uploadID, err := newUploadID()
	if err != nil {
		return CreateMultipartUploadResult{}, err
	}

	s.multipartMu.Lock()
	defer s.multipartMu.Unlock()
	s.multipartSeq++
	s.multipartUploads[uploadID] = &multipartUpload{
		bucket:      input.Bucket,
		key:         input.Key,
		contentType: input.ContentType,
		createdAt:   time.Now().UTC(),
		seq:         s.multipartSeq,
		parts:       map[int]multipartPart{},
	}
	s.evictOldestMultipartUploadLocked()
	return CreateMultipartUploadResult{UploadID: uploadID}, nil
}

func newUploadID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

func (s *ObjectStore) evictOldestMultipartUploadLocked() {
	if len(s.multipartUploads) <= maxMultipartUploads {
		return
	}
	oldestID := ""
	var oldestSeq uint64
	for uploadID, upload := range s.multipartUploads {
		if oldestID == "" || upload.seq < oldestSeq {
			oldestID = uploadID
			oldestSeq = upload.seq
		}
	}
	if oldestID != "" {
		s.logMultipartOrphansLocked(oldestID, "evict")
		delete(s.multipartUploads, oldestID)
	}
}

func (s *ObjectStore) logMultipartOrphansLocked(uploadID, reason string) {
	if s.logger == nil {
		return
	}
	upload := s.multipartUploads[uploadID]
	if upload == nil {
		return
	}
	records := make([]uploadRecord, 0)
	for _, part := range upload.parts {
		for _, chunk := range part.chunks {
			records = append(records, uploadRecord{FileID: chunk.telegramFileID, MessageID: chunk.telegramMessageID})
		}
	}
	if len(records) > 0 {
		s.logOrphanUpload(upload.bucket, upload.key, records, fmt.Errorf("multipart upload %s: %s", uploadID, reason))
	}
}

func (s *ObjectStore) UploadPart(ctx context.Context, input UploadPartInput) (UploadPartResult, error) {
	if input.PartNumber < 1 || input.UploadID == "" || input.Size < 0 {
		return UploadPartResult{}, ErrInvalidArgument
	}
	if input.Body == nil {
		input.Body = strings.NewReader("")
	}

	s.multipartMu.Lock()
	upload := s.multipartUploads[input.UploadID]
	if upload == nil || upload.bucket != input.Bucket || upload.key != input.Key {
		s.multipartMu.Unlock()
		return UploadPartResult{}, ErrNoSuchUpload
	}
	contentType := upload.contentType
	s.multipartMu.Unlock()

	releaseUpload := s.acquire(ctx, s.uploads)
	if releaseUpload == nil {
		return UploadPartResult{}, ctx.Err()
	}
	defer releaseUpload()

	chunks, etag, md5Bytes, size, err := s.uploadMultipartPartChunks(ctx, input, contentType)
	if err != nil {
		return UploadPartResult{}, err
	}

	s.multipartMu.Lock()
	defer s.multipartMu.Unlock()
	upload = s.multipartUploads[input.UploadID]
	if upload == nil || upload.bucket != input.Bucket || upload.key != input.Key {
		return UploadPartResult{}, ErrNoSuchUpload
	}
	if old, ok := upload.parts[input.PartNumber]; ok && len(old.chunks) > 0 {
		s.logMultipartPartOrphans(input.Bucket, input.Key, old, fmt.Errorf("multipart part %d replaced", input.PartNumber))
	}
	upload.parts[input.PartNumber] = multipartPart{
		number:   input.PartNumber,
		etag:     etag,
		md5Bytes: md5Bytes,
		size:     size,
		chunks:   chunks,
	}
	return UploadPartResult{ETag: etag}, nil
}

func (s *ObjectStore) uploadMultipartPartChunks(ctx context.Context, input UploadPartInput, contentType string) ([]multipartChunk, string, []byte, int64, error) {
	md5Hash := md5.New()
	chunkSize := s.options.Upload.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultUploadConfig().ChunkSize
	}
	limit := s.options.Upload.TypeLimits[telegram.TypeDocument]
	if limit > 0 && chunkSize > limit {
		chunkSize = limit
	}
	if chunkSize <= 0 {
		return nil, "", nil, 0, ErrEntityTooLarge
	}

	chunks := []multipartChunk{}
	uploads := []uploadRecord{}
	remaining := input.Size
	partIndex := 1
	for remaining > 0 {
		thisChunkSize := chunkSize
		if remaining < thisChunkSize {
			thisChunkSize = remaining
		}
		data := make([]byte, thisChunkSize)
		read, err := io.ReadFull(input.Body, data)
		if err != nil {
			if len(uploads) > 0 {
				s.logOrphanUpload(input.Bucket, input.Key, uploads, err)
			}
			return nil, "", nil, 0, err
		}
		if int64(read) != thisChunkSize {
			return nil, "", nil, 0, fmt.Errorf("copied %d bytes, want %d", read, thisChunkSize)
		}
		partData := data[:read]
		_, _ = md5Hash.Write(partData)
		chunkSHA := sha256.Sum256(partData)
		totalParts := int((input.Size + chunkSize - 1) / chunkSize)
		binding, err := s.bucketBinding(input.Bucket)
		if err != nil {
			if len(uploads) > 0 {
				s.logOrphanUpload(input.Bucket, input.Key, uploads, err)
			}
			return nil, "", nil, 0, err
		}
		uploaded, err := s.uploadTelegram(ctx, binding, telegram.UploadRequest{
			Type:     telegram.TypeDocument,
			ChatID:   binding.ChatID,
			Reader:   bytes.NewReader(partData),
			Filename: path.Base(input.Key),
			MIMEType: contentType,
			Caption:  s.renderCaption(PutObjectInput{Bucket: input.Bucket, Key: input.Key, ContentType: contentType, Size: input.Size}, partIndex, totalParts),
		})
		if err != nil {
			if len(uploads) > 0 {
				s.logOrphanUpload(input.Bucket, input.Key, uploads, err)
			}
			return nil, "", nil, 0, err
		}
		uploads = append(uploads, uploadRecord{FileID: uploaded.FileID, MessageID: uploaded.MessageID})
		chunks = append(chunks, multipartChunk{
			size:                 int64(len(partData)),
			telegramType:         uploaded.Type,
			telegramFileID:       uploaded.FileID,
			telegramMessageID:    uploaded.MessageID,
			telegramFileUniqueID: uploaded.FileUniqueID,
			sha256:               hex.EncodeToString(chunkSHA[:]),
		})
		remaining -= int64(len(partData))
		partIndex++
	}
	if extra, err := io.ReadAll(input.Body); err != nil {
		return nil, "", nil, 0, err
	} else if len(extra) > 0 {
		return nil, "", nil, 0, fmt.Errorf("copied %d bytes, want %d", input.Size+int64(len(extra)), input.Size)
	}
	md5Bytes := md5Hash.Sum(nil)
	return chunks, hex.EncodeToString(md5Bytes), md5Bytes, input.Size, nil
}

func (s *ObjectStore) logMultipartPartOrphans(bucket, key string, part multipartPart, err error) {
	records := make([]uploadRecord, 0, len(part.chunks))
	for _, chunk := range part.chunks {
		records = append(records, uploadRecord{FileID: chunk.telegramFileID, MessageID: chunk.telegramMessageID})
	}
	if len(records) > 0 {
		s.logOrphanUpload(bucket, key, records, err)
	}
}

func (s *ObjectStore) CompleteMultipartUpload(ctx context.Context, input CompleteMultipartUploadInput) (CompleteMultipartUploadResult, error) {
	if input.UploadID == "" || len(input.Parts) == 0 {
		return CompleteMultipartUploadResult{}, ErrInvalidArgument
	}

	releaseLock := s.locker.Lock(input.Bucket, input.Key)
	defer releaseLock()

	s.multipartMu.Lock()
	upload := s.multipartUploads[input.UploadID]
	if upload == nil || upload.bucket != input.Bucket || upload.key != input.Key {
		s.multipartMu.Unlock()
		return CompleteMultipartUploadResult{}, ErrNoSuchUpload
	}
	parts, err := upload.completeParts(input.Parts)
	if err != nil {
		s.multipartMu.Unlock()
		return CompleteMultipartUploadResult{}, err
	}
	contentType := upload.contentType
	s.multipartMu.Unlock()

	object, chunks, etag := buildMultipartObject(input.Bucket, input.Key, contentType, parts)
	if err := s.meta.PutObject(ctx, object, chunks); err != nil {
		s.logMetadataPutObject(input.Bucket, input.Key, len(chunks), etag, err)
		return CompleteMultipartUploadResult{}, err
	}
	s.logMetadataPutObject(input.Bucket, input.Key, len(chunks), etag, nil)

	s.multipartMu.Lock()
	delete(s.multipartUploads, input.UploadID)
	s.multipartMu.Unlock()
	return CompleteMultipartUploadResult{ETag: etag}, nil
}

func (u *multipartUpload) completeParts(requested []CompletedPart) ([]multipartPart, error) {
	parts := make([]multipartPart, 0, len(requested))
	lastPart := 0
	for _, requestedPart := range requested {
		if requestedPart.PartNumber <= lastPart {
			return nil, ErrInvalidPartOrder
		}
		lastPart = requestedPart.PartNumber
		part, ok := u.parts[requestedPart.PartNumber]
		if !ok || !etagMatches(part.etag, requestedPart.ETag) {
			return nil, ErrInvalidPart
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func etagMatches(stored, submitted string) bool {
	return strings.Trim(submitted, "\"") == strings.Trim(stored, "\"")
}

func buildMultipartObject(bucket, key, contentType string, parts []multipartPart) (metadata.Object, []metadata.Chunk, string) {
	size := int64(0)
	chunkCount := 0
	for _, part := range parts {
		size += part.size
		chunkCount += len(part.chunks)
	}
	etag := multipartETag(parts)
	chunks := make([]metadata.Chunk, 0, chunkCount)
	offset := int64(0)
	partNumber := 1
	for _, part := range parts {
		for _, chunk := range part.chunks {
			chunks = append(chunks, metadata.Chunk{
				Bucket:               bucket,
				Key:                  key,
				PartNumber:           partNumber,
				Offset:               offset,
				Size:                 chunk.size,
				TelegramType:         chunk.telegramType,
				TelegramFileID:       chunk.telegramFileID,
				TelegramMessageID:    chunk.telegramMessageID,
				TelegramFileUniqueID: chunk.telegramFileUniqueID,
				SHA256:               chunk.sha256,
			})
			offset += chunk.size
			partNumber++
		}
	}
	object := metadata.Object{
		Bucket:         bucket,
		Key:            key,
		Size:           size,
		ContentType:    contentType,
		ETag:           etag,
		SHA256:         "",
		LastModified:   time.Now().UTC(),
		ChunkCount:     len(chunks),
		TelegramType:   telegram.TypeDocument,
		UploadStrategy: "multipart",
	}
	return object, chunks, etag
}

func multipartETag(parts []multipartPart) string {
	whole := md5.New()
	for _, part := range parts {
		_, _ = whole.Write(part.md5Bytes)
	}
	return fmt.Sprintf("%s-%d", hex.EncodeToString(whole.Sum(nil)), len(parts))
}

func (s *ObjectStore) AbortMultipartUpload(ctx context.Context, input AbortMultipartUploadInput) error {
	if input.UploadID == "" {
		return ErrInvalidArgument
	}
	s.multipartMu.Lock()
	defer s.multipartMu.Unlock()
	upload := s.multipartUploads[input.UploadID]
	if upload == nil || upload.bucket != input.Bucket || upload.key != input.Key {
		return ErrNoSuchUpload
	}
	s.logMultipartOrphansLocked(input.UploadID, "abort")
	delete(s.multipartUploads, input.UploadID)
	return nil
}

func (s *ObjectStore) HeadObject(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	if err := s.HeadBucket(ctx, bucket); err != nil {
		return ObjectInfo{}, err
	}
	object, err := s.meta.HeadObject(ctx, bucket, key)
	if err != nil {
		if err == metadata.ErrNotFound {
			return ObjectInfo{}, ErrNoSuchKey
		}
		return ObjectInfo{}, err
	}
	return objectInfoFromMetadata(object), nil
}

func (s *ObjectStore) GetObject(ctx context.Context, input GetObjectInput) (io.ReadCloser, ObjectInfo, error) {
	if err := s.HeadBucket(ctx, input.Bucket); err != nil {
		return nil, ObjectInfo{}, err
	}
	ctx, cancel := context.WithCancel(ctx)
	object, chunks, err := s.meta.GetObject(ctx, input.Bucket, input.Key)
	if err != nil {
		cancel()
		if err == metadata.ErrNotFound {
			return nil, ObjectInfo{}, ErrNoSuchKey
		}
		return nil, ObjectInfo{}, err
	}
	info := objectInfoFromMetadata(object)

	selected := make([]SelectedChunk, 0, len(chunks))
	if input.Range == nil {
		selected = make([]SelectedChunk, 0, len(chunks))
		for _, chunk := range chunks {
			selected = append(selected, SelectedChunk{ChunkRef: ChunkRef{Part: chunk.PartNumber, FileID: chunk.TelegramFileID, Offset: chunk.Offset, Size: chunk.Size}, Take: chunk.Size})
		}
	} else {
		selected = SelectChunksForRange(chunkRefsFromMetadata(chunks), *input.Range)
	}

	releaseDownload := s.acquire(ctx, s.downloads)
	if releaseDownload == nil {
		cancel()
		return nil, ObjectInfo{}, ctx.Err()
	}

	pr, pw := io.Pipe()
	go func() {
		defer cancel()
		defer releaseDownload()
		defer pw.Close()
		binding, err := s.bucketBinding(input.Bucket)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		for _, chunk := range selected {
			reader, err := s.downloadChunk(ctx, binding, chunk.FileID)
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}

			var source io.Reader = reader
			if chunk.Skip > 0 {
				source = io.LimitReader(source, chunk.Skip)
				if _, err := io.Copy(io.Discard, source); err != nil {
					_ = reader.Close()
					_ = pw.CloseWithError(err)
					return
				}
				source = reader
			}
			if chunk.Take >= 0 {
				source = io.LimitReader(source, chunk.Take)
			}
			if _, err := io.Copy(pw, source); err != nil {
				_ = reader.Close()
				_ = pw.CloseWithError(err)
				return
			}
			if err := reader.Close(); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}
	}()

	return &cancelReadCloser{ReadCloser: pr, cancel: cancel}, info, nil
}

func (s *ObjectStore) ListObjects(ctx context.Context, input ListObjectsInput) (ListObjectsResult, error) {
	if err := s.HeadBucket(ctx, input.Bucket); err != nil {
		return ListObjectsResult{}, err
	}
	limit := input.Limit
	if limit < 0 {
		limit = 1000
	}

	result := ListObjectsResult{}
	seenPrefixes := map[string]struct{}{}
	afterKey := input.AfterKey
	truncated := false
	stopListing := false

	for len(result.Objects)+len(result.CommonPrefixes) < limit {
		remaining := limit - (len(result.Objects) + len(result.CommonPrefixes))
		queryLimit := remaining*4 + 1
		if queryLimit < 2 {
			queryLimit = 2
		}
		objects, err := s.meta.ListObjects(ctx, metadata.ListQuery{Bucket: input.Bucket, Prefix: input.Prefix, AfterKey: afterKey, Limit: queryLimit})
		if err != nil {
			return ListObjectsResult{}, err
		}
		if len(objects) == 0 {
			break
		}

		pageFull := false
		for _, object := range objects {
			emitCount := len(result.Objects) + len(result.CommonPrefixes)
			if input.Delimiter == "" {
				if emitCount >= limit {
					pageFull = true
					break
				}
				result.Objects = append(result.Objects, objectInfoFromMetadata(object))
				result.NextContinuationAfter = object.Key
				afterKey = object.Key
				if len(result.Objects)+len(result.CommonPrefixes) >= limit {
					pageFull = true
					break
				}
				continue
			}

			remainder := strings.TrimPrefix(object.Key, input.Prefix)
			idx := strings.Index(remainder, input.Delimiter)
			if idx < 0 {
				if emitCount >= limit {
					pageFull = true
					break
				}
				result.Objects = append(result.Objects, objectInfoFromMetadata(object))
				result.NextContinuationAfter = object.Key
				afterKey = object.Key
				if len(result.Objects)+len(result.CommonPrefixes) >= limit {
					pageFull = true
					break
				}
				continue
			}

			prefix := input.Prefix + remainder[:idx] + input.Delimiter
			if _, ok := seenPrefixes[prefix]; ok {
				result.NextContinuationAfter = object.Key
				afterKey = object.Key
				continue
			}
			if emitCount >= limit {
				pageFull = true
				break
			}
			seenPrefixes[prefix] = struct{}{}
			result.CommonPrefixes = append(result.CommonPrefixes, prefix)
			result.NextContinuationAfter = object.Key
			afterKey = object.Key
			if len(result.Objects)+len(result.CommonPrefixes) >= limit {
				lastKey, hasMore, err := s.advanceContinuationPastPrefix(ctx, input, prefix, afterKey)
				if err != nil {
					return ListObjectsResult{}, err
				}
				result.NextContinuationAfter = lastKey
				afterKey = lastKey
				truncated = hasMore
				stopListing = true
				break
			}
		}

		if stopListing {
			break
		}
		if pageFull {
			break
		}
		if len(objects) < queryLimit {
			break
		}
	}

	if result.NextContinuationAfter != "" {
		if !truncated {
			next, err := s.meta.ListObjects(ctx, metadata.ListQuery{Bucket: input.Bucket, Prefix: input.Prefix, AfterKey: result.NextContinuationAfter, Limit: 1})
			if err != nil {
				return ListObjectsResult{}, err
			}
			truncated = len(next) > 0
		}
		result.IsTruncated = truncated
	}

	return result, nil
}

func (s *ObjectStore) advanceContinuationPastPrefix(ctx context.Context, input ListObjectsInput, prefix, afterKey string) (string, bool, error) {
	cursor := afterKey
	for {
		objects, err := s.meta.ListObjects(ctx, metadata.ListQuery{Bucket: input.Bucket, Prefix: input.Prefix, AfterKey: cursor, Limit: 32})
		if err != nil {
			return "", false, err
		}
		if len(objects) == 0 {
			return cursor, false, nil
		}
		for _, object := range objects {
			remainder := strings.TrimPrefix(object.Key, input.Prefix)
			idx := strings.Index(remainder, input.Delimiter)
			if idx < 0 {
				return cursor, true, nil
			}
			currentPrefix := input.Prefix + remainder[:idx] + input.Delimiter
			if currentPrefix != prefix {
				return cursor, true, nil
			}
			cursor = object.Key
		}
		if len(objects) < 32 {
			return cursor, false, nil
		}
	}
}

func (s *ObjectStore) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := s.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	if err := s.meta.DeleteObject(ctx, bucket, key); err != nil {
		if err == metadata.ErrNotFound {
			return nil
		}
		return err
	}
	return nil
}

func HumanSize(size int64) string {
	if size < 1024 {
		return fmt.Sprintf("%d B", size)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	value := float64(size)
	for _, unit := range units {
		value /= 1024
		if value < 1024 {
			return fmt.Sprintf("%.1f %s", value, unit)
		}
	}
	return fmt.Sprintf("%.1f PiB", value/1024)
}

func (s *ObjectStore) putEmpty(ctx context.Context, input PutObjectInput, strategy UploadStrategy) (PutObjectResult, error) {
	etag := hex.EncodeToString(md5.New().Sum(nil))
	shaSum := hex.EncodeToString(sha256.New().Sum(nil))
	object := metadata.Object{
		Bucket:         input.Bucket,
		Key:            input.Key,
		Size:           0,
		ContentType:    input.ContentType,
		ETag:           etag,
		SHA256:         shaSum,
		LastModified:   time.Now().UTC(),
		ChunkCount:     0,
		TelegramType:   strategy.TelegramType,
		UploadStrategy: strategy.UploadStrategy,
	}
	if err := s.meta.PutObject(ctx, object, nil); err != nil {
		s.logMetadataPutObject(input.Bucket, input.Key, 0, etag, err)
		return PutObjectResult{}, err
	}
	s.logMetadataPutObject(input.Bucket, input.Key, 0, etag, nil)
	return PutObjectResult{ETag: etag}, nil
}

func (s *ObjectStore) stageSingleUpload(input PutObjectInput, md5Hash hash.Hash, shaHash hash.Hash) (*stagedUpload, error) {
	bufferSize := s.options.Upload.PutBufferSize
	if bufferSize <= 0 {
		bufferSize = DefaultUploadConfig().PutBufferSize
	}
	if input.Size <= int64(bufferSize) {
		data, err := io.ReadAll(io.TeeReader(input.Body, io.MultiWriter(md5Hash, shaHash)))
		if err != nil {
			return nil, err
		}
		if int64(len(data)) != input.Size {
			return nil, fmt.Errorf("copied %d bytes, want %d", len(data), input.Size)
		}
		return &stagedUpload{
			reader: bytes.NewReader(data),
			close:  func() error { return nil },
			remove: func() error { return nil },
		}, nil
	}

	file, err := os.CreateTemp("", "tgnas-upload-*")
	if err != nil {
		return nil, err
	}
	written, err := io.Copy(file, io.TeeReader(input.Body, io.MultiWriter(md5Hash, shaHash)))
	if err != nil {
		file.Close()
		os.Remove(file.Name())
		return nil, err
	}
	if written != input.Size {
		file.Close()
		os.Remove(file.Name())
		return nil, fmt.Errorf("copied %d bytes, want %d", written, input.Size)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		os.Remove(file.Name())
		return nil, err
	}
	return &stagedUpload{
		reader: file,
		close:  file.Close,
		remove: func() error { return os.Remove(file.Name()) },
	}, nil
}

func removeStagingArtifact(logger *log.Logger, staged *stagedUpload) {
	if staged == nil {
		return
	}
	if err := staged.close(); err != nil && logger != nil {
		logger.Printf("debug event=upload_staging_close result=error error=%q", sanitizeLogError(err))
	}
	if err := staged.remove(); err != nil && logger != nil {
		logger.Printf("debug event=upload_staging_cleanup result=error error=%q", sanitizeLogError(err))
	}
}

func (s *ObjectStore) putSingle(ctx context.Context, input PutObjectInput, strategy UploadStrategy) (PutObjectResult, error) {
	now := time.Now().UTC()
	md5Hash := md5.New()
	shaHash := sha256.New()
	staged, err := s.stageSingleUpload(input, md5Hash, shaHash)
	if err != nil {
		return PutObjectResult{}, err
	}
	defer removeStagingArtifact(s.logger, staged)

	binding, err := s.bucketBinding(input.Bucket)
	if err != nil {
		return PutObjectResult{}, err
	}
	caption := s.renderCaption(input, 1, 1)
	s.logger.Printf("debug event=telegram_upload_part bucket=%q key=%q part=%d parts=%d media_type=%q", input.Bucket, input.Key, 1, 1, strategy.TelegramType)
	uploaded, err := s.uploadTelegram(ctx, binding, telegram.UploadRequest{
		Type:     strategy.TelegramType,
		ChatID:   binding.ChatID,
		Reader:   staged.reader,
		Filename: path.Base(input.Key),
		MIMEType: input.ContentType,
		Caption:  caption,
	})
	if err != nil {
		return PutObjectResult{}, err
	}
	s.logger.Printf("debug event=telegram_upload_part_result bucket=%q key=%q part=%d message_id=%d file_id_returned=%t", input.Bucket, input.Key, 1, uploaded.MessageID, uploaded.FileID != "")

	storedSize := input.Size
	etag := hex.EncodeToString(md5Hash.Sum(nil))
	shaSum := hex.EncodeToString(shaHash.Sum(nil))
	if strategy.UploadStrategy == "typed" && uploaded.FileSize > 0 {
		storedSize = uploaded.FileSize
		// Typed uploads (photo/video/audio/animation) may be recompressed by
		// Telegram, so the hash of original input bytes doesn't match the actual
		// downloadable content. Use a random ETag with "-typed" suffix so S3
		// clients don't attempt MD5 verification (same pattern as multipart ETags).
		var randBuf [16]byte
		_, _ = rand.Read(randBuf[:])
		etag = hex.EncodeToString(randBuf[:]) + "-typed"
		shaSum = ""
	}
	object := metadata.Object{
		Bucket:         input.Bucket,
		Key:            input.Key,
		Size:           storedSize,
		ContentType:    input.ContentType,
		ETag:           etag,
		SHA256:         shaSum,
		LastModified:   now,
		ChunkCount:     1,
		TelegramType:   uploaded.Type,
		UploadStrategy: strategy.UploadStrategy,
	}
	chunks := []metadata.Chunk{{
		Bucket:               input.Bucket,
		Key:                  input.Key,
		PartNumber:           1,
		Offset:               0,
		Size:                 storedSize,
		TelegramType:         uploaded.Type,
		TelegramFileID:       uploaded.FileID,
		TelegramMessageID:    uploaded.MessageID,
		TelegramFileUniqueID: uploaded.FileUniqueID,
		SHA256:               shaSum,
	}}
	if err := s.meta.PutObject(ctx, object, chunks); err != nil {
		s.logMetadataPutObject(input.Bucket, input.Key, len(chunks), etag, err)
		s.logOrphanUpload(input.Bucket, input.Key, []uploadRecord{{FileID: uploaded.FileID, MessageID: uploaded.MessageID}}, err)
		return PutObjectResult{}, err
	}
	s.logMetadataPutObject(input.Bucket, input.Key, len(chunks), etag, nil)
	return PutObjectResult{ETag: etag}, nil
}

func (s *ObjectStore) putChunked(ctx context.Context, input PutObjectInput, strategy UploadStrategy) (PutObjectResult, error) {
	wholeMD5 := md5.New()
	wholeSHA := sha256.New()
	now := time.Now().UTC()
	chunkSize := strategy.ChunkSize
	if chunkSize <= 0 {
		chunkSize = s.options.Upload.ChunkSize
	}
	parts := int((input.Size + chunkSize - 1) / chunkSize)
	s.logger.Printf("debug event=put_object_chunking bucket=%q key=%q size=%d chunk_size=%d chunk_count=%d", input.Bucket, input.Key, input.Size, chunkSize, parts)
	chunks := make([]metadata.Chunk, 0, parts)
	uploads := make([]uploadRecord, 0, parts)
	offset := int64(0)
	remaining := input.Size
	part := 1

	for remaining > 0 {
		thisChunkSize := chunkSize
		if remaining < thisChunkSize {
			thisChunkSize = remaining
		}
		data := make([]byte, thisChunkSize)
		read, err := io.ReadFull(input.Body, data)
		if err != nil {
			return PutObjectResult{}, err
		}
		if int64(read) != thisChunkSize {
			return PutObjectResult{}, fmt.Errorf("copied %d bytes, want %d", read, thisChunkSize)
		}
		partData := data[:read]
		_, _ = wholeMD5.Write(partData)
		_, _ = wholeSHA.Write(partData)
		chunkSHA := sha256.Sum256(partData)
		s.logger.Printf("debug event=telegram_upload_part bucket=%q key=%q part=%d parts=%d media_type=%q", input.Bucket, input.Key, part, parts, telegram.TypeDocument)
		binding, err := s.bucketBinding(input.Bucket)
		if err != nil {
			if len(uploads) > 0 {
				s.logOrphanUpload(input.Bucket, input.Key, uploads, err)
			}
			return PutObjectResult{}, err
		}
		uploaded, err := s.uploadTelegram(ctx, binding, telegram.UploadRequest{
			Type:     telegram.TypeDocument,
			ChatID:   binding.ChatID,
			Reader:   bytes.NewReader(partData),
			Filename: path.Base(input.Key),
			MIMEType: input.ContentType,
			Caption:  s.renderCaption(input, part, parts),
		})
		if err != nil {
			if len(uploads) > 0 {
				s.logOrphanUpload(input.Bucket, input.Key, uploads, err)
			}
			return PutObjectResult{}, err
		}
		s.logger.Printf("debug event=telegram_upload_part_result bucket=%q key=%q part=%d message_id=%d file_id_returned=%t", input.Bucket, input.Key, part, uploaded.MessageID, uploaded.FileID != "")
		chunks = append(chunks, metadata.Chunk{
			Bucket:               input.Bucket,
			Key:                  input.Key,
			PartNumber:           part,
			Offset:               offset,
			Size:                 int64(len(partData)),
			TelegramType:         uploaded.Type,
			TelegramFileID:       uploaded.FileID,
			TelegramMessageID:    uploaded.MessageID,
			TelegramFileUniqueID: uploaded.FileUniqueID,
			SHA256:               hex.EncodeToString(chunkSHA[:]),
		})
		uploads = append(uploads, uploadRecord{FileID: uploaded.FileID, MessageID: uploaded.MessageID})
		offset += int64(len(partData))
		remaining -= int64(len(partData))
		part++
	}
	if extra, err := io.ReadAll(input.Body); err != nil {
		return PutObjectResult{}, err
	} else if len(extra) > 0 {
		return PutObjectResult{}, fmt.Errorf("copied %d bytes, want %d", input.Size+int64(len(extra)), input.Size)
	}
	etag := hex.EncodeToString(wholeMD5.Sum(nil))
	shaSum := hex.EncodeToString(wholeSHA.Sum(nil))
	object := metadata.Object{
		Bucket:         input.Bucket,
		Key:            input.Key,
		Size:           input.Size,
		ContentType:    input.ContentType,
		ETag:           etag,
		SHA256:         shaSum,
		LastModified:   now,
		ChunkCount:     len(chunks),
		TelegramType:   telegram.TypeDocument,
		UploadStrategy: strategy.UploadStrategy,
	}
	if err := s.meta.PutObject(ctx, object, chunks); err != nil {
		s.logMetadataPutObject(input.Bucket, input.Key, len(chunks), etag, err)
		s.logOrphanUpload(input.Bucket, input.Key, uploads, err)
		return PutObjectResult{}, err
	}
	s.logMetadataPutObject(input.Bucket, input.Key, len(chunks), etag, nil)
	return PutObjectResult{ETag: etag}, nil
}

func (s *ObjectStore) bucketBinding(name string) (BucketBinding, error) {
	binding, ok := s.bucketBindings[name]
	if !ok {
		if item, ok := s.startupBuckets[name]; ok && s.tg != nil {
			return BucketBinding{Name: name, ChatID: item.ChatID, TokenKey: "", Telegram: s.tg}, nil
		}
		return BucketBinding{}, ErrNoSuchBucket
	}
	if binding.ChatID == "" {
		if item, ok := s.startupBuckets[name]; ok {
			binding.ChatID = item.ChatID
		}
	}
	if binding.Telegram == nil && s.tg != nil {
		binding.Telegram = s.tg
	}
	return binding, nil
}

func (s *ObjectStore) renderCaption(input PutObjectInput, part, parts int) string {
	if s.options.Caption == nil {
		return ""
	}
	return s.options.Caption.Render(telegram.CaptionData{
		Bucket: input.Bucket,
		Key:    input.Key,
		Name:   path.Base(input.Key),
		Size:   HumanSize(input.Size),
		Bytes:  input.Size,
		Part:   part,
		Parts:  parts,
	})
}

func (s *ObjectStore) uploadTelegram(ctx context.Context, binding BucketBinding, request telegram.UploadRequest) (telegram.UploadedFile, error) {
	gate, ok := s.uploadGates[binding.TokenKey]
	if !ok {
		return telegram.UploadedFile{}, fmt.Errorf("missing upload gate for token key %q", binding.TokenKey)
	}
	select {
	case <-gate.slot:
	case <-ctx.Done():
		return telegram.UploadedFile{}, ctx.Err()
	}
	defer func() { gate.slot <- struct{}{} }()

	for {
		gate.mu.Lock()
		wait := time.Until(gate.until)
		gate.mu.Unlock()
		if wait <= 0 {
			break
		}
		if err := sleepWithContext(ctx, wait); err != nil {
			return telegram.UploadedFile{}, err
		}
	}

	uploaded, err := binding.Telegram.Upload(ctx, request)
	if err != nil {
		if retryAfter, ok := telegram.IsRateLimitError(err); ok && retryAfter > 0 {
			gate.mu.Lock()
			gate.until = time.Now().Add(retryAfter)
			gate.mu.Unlock()
		}
		if shouldMarkBucketUnavailable(err) {
			s.markBucketUnavailable(binding.Name)
		}
		return telegram.UploadedFile{}, err
	}
	return uploaded, nil
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *ObjectStore) downloadChunk(ctx context.Context, binding BucketBinding, fileID string) (io.ReadCloser, error) {
	release := s.acquire(ctx, s.telegramSem)
	if release == nil {
		return nil, ctx.Err()
	}
	reader, err := binding.Telegram.Download(ctx, fileID)
	if err != nil {
		release()
		return nil, err
	}
	return &releaseReadCloser{ReadCloser: reader, release: release}, nil
}

func (s *ObjectStore) acquire(ctx context.Context, sem chan struct{}) func() {
	if sem == nil {
		return func() {}
	}
	select {
	case sem <- struct{}{}:
		return func() { <-sem }
	case <-ctx.Done():
		return nil
	}
}

func (s *ObjectStore) logMetadataPutObject(bucket, key string, chunkCount int, etag string, err error) {
	if s.logger == nil {
		return
	}
	if err != nil {
		s.logger.Printf("debug event=metadata_put_object bucket=%q key=%q chunk_count=%d etag=%q result=error error=%q", bucket, key, chunkCount, etag, sanitizeLogError(err))
		return
	}
	s.logger.Printf("debug event=metadata_put_object bucket=%q key=%q chunk_count=%d etag=%q result=success", bucket, key, chunkCount, etag)
}

func (s *ObjectStore) logOrphanUpload(bucket, key string, uploads []uploadRecord, err error) {
	if s.logger == nil {
		return
	}
	parts := make([]string, 0, len(uploads))
	for _, upload := range uploads {
		parts = append(parts, fmt.Sprintf("file_id=%s message_id=%d", summarizeFileID(upload.FileID), upload.MessageID))
	}
	s.logger.Printf("debug event=orphan_upload bucket=%q key=%q uploads=[%s] error=%q", bucket, key, strings.Join(parts, " "), sanitizeLogError(err))
}

func summarizeFileID(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= 8 {
		return "[redacted]"
	}
	return value[:4] + "..." + value[len(value)-4:]
}

var sensitiveAssignmentPattern = regexp.MustCompile(`(?i)\b(bot_token|secret_key)\b\s*([=:])\s*([^\s,;]+)`)

func sanitizeLogError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	return sensitiveAssignmentPattern.ReplaceAllString(message, `$1$2[REDACTED]`)
}

func objectInfoFromMetadata(object metadata.Object) ObjectInfo {
	return ObjectInfo{
		Bucket:       object.Bucket,
		Key:          object.Key,
		Size:         object.Size,
		ContentType:  object.ContentType,
		ETag:         object.ETag,
		SHA256:       object.SHA256,
		LastModified: object.LastModified,
	}
}

func chunkRefsFromMetadata(chunks []metadata.Chunk) []ChunkRef {
	refs := make([]ChunkRef, 0, len(chunks))
	for _, chunk := range chunks {
		refs = append(refs, ChunkRef{Part: chunk.PartNumber, FileID: chunk.TelegramFileID, Offset: chunk.Offset, Size: chunk.Size})
	}
	return refs
}

type releaseReadCloser struct {
	io.ReadCloser
	release func()
}

func (r *releaseReadCloser) Close() error {
	err := r.ReadCloser.Close()
	if r.release != nil {
		r.release()
		r.release = nil
	}
	return err
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel func()
}

func (r *cancelReadCloser) Close() error {
	err := r.ReadCloser.Close()
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	return err
}

func (s *ObjectStore) bucketChatID(bucket string) string {
	binding, err := s.bucketBinding(bucket)
	if err != nil {
		return ""
	}
	return binding.ChatID
}
