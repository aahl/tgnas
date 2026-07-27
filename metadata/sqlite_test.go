package metadata

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestSQLiteReadOnlyDSN(t *testing.T) {
	path := "/tmp/metadata.sqlite"
	want := "file:///tmp/metadata.sqlite?mode=ro"
	if runtime.GOOS == "windows" {
		path = `C:\Users\test\metadata.sqlite`
		want = "file:///C:/Users/test/metadata.sqlite?mode=ro"
	}

	if got := sqliteReadOnlyDSN(path); got != want {
		t.Fatalf("sqliteReadOnlyDSN(%q) = %q, want %q", path, got, want)
	}
}

func TestSQLiteBucketsObjectsAndChunks(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	bucket := Bucket{
		Name:      "photos",
		ChatID:    "-100123",
		CreatedAt: time.Unix(10, 0),
		Enabled:   true,
	}
	if err := store.UpsertBucket(t.Context(), bucket); err != nil {
		t.Fatalf("UpsertBucket returned error: %v", err)
	}

	gotBucket, err := store.GetBucket(t.Context(), "photos")
	if err != nil {
		t.Fatalf("GetBucket returned error: %v", err)
	}
	if gotBucket != bucket {
		t.Fatalf("bucket = %+v, want %+v", gotBucket, bucket)
	}

	object := Object{
		Bucket:         "photos",
		Key:            "b/cat.jpg",
		Size:           11,
		ContentType:    "image/jpeg",
		ETag:           "5eb63bbbe01eeed093cb22bb8f5acdc3",
		SHA256:         "sha",
		LastModified:   time.Unix(20, 0),
		ChunkCount:     2,
		TelegramType:   "document",
		UploadStrategy: "chunked_document",
	}
	chunks := []Chunk{
		{
			Bucket:               "photos",
			Key:                  "b/cat.jpg",
			PartNumber:           1,
			Offset:               0,
			Size:                 5,
			TelegramType:         "document",
			TelegramFileID:       "file-1",
			TelegramMessageID:    101,
			TelegramFileUniqueID: "u1",
			SHA256:               "c1",
		},
		{
			Bucket:               "photos",
			Key:                  "b/cat.jpg",
			PartNumber:           2,
			Offset:               5,
			Size:                 6,
			TelegramType:         "document",
			TelegramFileID:       "file-2",
			TelegramMessageID:    102,
			TelegramFileUniqueID: "u2",
			SHA256:               "c2",
		},
	}
	if err := store.PutObject(t.Context(), object, chunks); err != nil {
		t.Fatalf("PutObject returned error: %v", err)
	}

	gotObject, gotChunks, err := store.GetObject(t.Context(), "photos", "b/cat.jpg")
	if err != nil {
		t.Fatalf("GetObject returned error: %v", err)
	}
	if !reflect.DeepEqual(gotObject, object) {
		t.Fatalf("object = %+v, want %+v", gotObject, object)
	}
	if !reflect.DeepEqual(gotChunks, chunks) {
		t.Fatalf("chunks = %+v, want %+v", gotChunks, chunks)
	}
}

func TestSQLitePutObjectReplacesChunks(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	object := Object{
		Bucket:         "photos",
		Key:            "b/cat.jpg",
		Size:           11,
		ContentType:    "image/jpeg",
		ETag:           "etag",
		SHA256:         "sha",
		LastModified:   time.Unix(20, 0),
		ChunkCount:     2,
		TelegramType:   "document",
		UploadStrategy: "chunked_document",
	}
	if err := store.PutObject(t.Context(), object, []Chunk{
		{
			Bucket:               "photos",
			Key:                  "b/cat.jpg",
			PartNumber:           1,
			Offset:               0,
			Size:                 5,
			TelegramType:         "document",
			TelegramFileID:       "file-1",
			TelegramMessageID:    101,
			TelegramFileUniqueID: "u1",
			SHA256:               "c1",
		},
		{
			Bucket:               "photos",
			Key:                  "b/cat.jpg",
			PartNumber:           2,
			Offset:               5,
			Size:                 6,
			TelegramType:         "document",
			TelegramFileID:       "file-2",
			TelegramMessageID:    102,
			TelegramFileUniqueID: "u2",
			SHA256:               "c2",
		},
	}); err != nil {
		t.Fatalf("initial PutObject returned error: %v", err)
	}

	replacement := object
	replacement.ChunkCount = 1
	if err := store.PutObject(t.Context(), replacement, []Chunk{
		{
			Bucket:               "photos",
			Key:                  "b/cat.jpg",
			PartNumber:           1,
			Offset:               0,
			Size:                 11,
			TelegramType:         "document",
			TelegramFileID:       "replacement",
			TelegramMessageID:    201,
			TelegramFileUniqueID: "u3",
			SHA256:               "c3",
		},
	}); err != nil {
		t.Fatalf("replacement PutObject returned error: %v", err)
	}

	gotObject, gotChunks, err := store.GetObject(t.Context(), "photos", "b/cat.jpg")
	if err != nil {
		t.Fatalf("GetObject returned error: %v", err)
	}
	if gotObject.ChunkCount != 1 {
		t.Fatalf("chunk count = %d", gotObject.ChunkCount)
	}
	if len(gotChunks) != 1 {
		t.Fatalf("len(chunks) = %d", len(gotChunks))
	}
	if gotChunks[0].TelegramFileID != "replacement" {
		t.Fatalf("file id = %q", gotChunks[0].TelegramFileID)
	}
}

func TestSQLitePutObjectReplacementRollbackOnChunkInsertFailure(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	originalObject := Object{
		Bucket:         "photos",
		Key:            "b/cat.jpg",
		Size:           5,
		ContentType:    "image/jpeg",
		ETag:           "original-etag",
		SHA256:         "original-sha",
		LastModified:   time.Unix(21, 0),
		ChunkCount:     1,
		TelegramType:   "document",
		UploadStrategy: "document",
	}
	originalChunks := []Chunk{
		{
			Bucket:               "photos",
			Key:                  "b/cat.jpg",
			PartNumber:           1,
			Offset:               0,
			Size:                 5,
			TelegramType:         "document",
			TelegramFileID:       "original-file",
			TelegramMessageID:    101,
			TelegramFileUniqueID: "original-unique",
			SHA256:               "original-chunk-sha",
		},
	}
	if err := store.PutObject(t.Context(), originalObject, originalChunks); err != nil {
		t.Fatalf("initial PutObject returned error: %v", err)
	}

	replacementObject := Object{
		Bucket:         "photos",
		Key:            "b/cat.jpg",
		Size:           10,
		ContentType:    "image/jpeg",
		ETag:           "replacement-etag",
		SHA256:         "replacement-sha",
		LastModified:   time.Unix(22, 0),
		ChunkCount:     2,
		TelegramType:   "document",
		UploadStrategy: "chunked_document",
	}
	replacementChunks := []Chunk{
		{
			Bucket:               "photos",
			Key:                  "b/cat.jpg",
			PartNumber:           1,
			Offset:               0,
			Size:                 5,
			TelegramType:         "document",
			TelegramFileID:       "replacement-file-1",
			TelegramMessageID:    201,
			TelegramFileUniqueID: "replacement-unique-1",
			SHA256:               "replacement-chunk-sha-1",
		},
		{
			Bucket:               "photos",
			Key:                  "b/cat.jpg",
			PartNumber:           1,
			Offset:               5,
			Size:                 5,
			TelegramType:         "document",
			TelegramFileID:       "replacement-file-2",
			TelegramMessageID:    202,
			TelegramFileUniqueID: "replacement-unique-2",
			SHA256:               "replacement-chunk-sha-2",
		},
	}

	if err := store.PutObject(t.Context(), replacementObject, replacementChunks); err == nil {
		t.Fatal("replacement PutObject returned nil error, want primary key violation")
	}

	gotObject, gotChunks, err := store.GetObject(t.Context(), "photos", "b/cat.jpg")
	if err != nil {
		t.Fatalf("GetObject returned error: %v", err)
	}
	if !reflect.DeepEqual(gotObject, originalObject) {
		t.Fatalf("object = %+v, want %+v", gotObject, originalObject)
	}
	if !reflect.DeepEqual(gotChunks, originalChunks) {
		t.Fatalf("chunks = %+v, want %+v", gotChunks, originalChunks)
	}
}

func TestSQLiteListObjectsOrdersAndPaginates(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	objects := []Object{
		{
			Bucket:         "photos",
			Key:            "a/one.txt",
			Size:           1,
			ContentType:    "text/plain",
			ETag:           "etag-1",
			SHA256:         "sha-1",
			LastModified:   time.Unix(1, 0),
			ChunkCount:     1,
			TelegramType:   "document",
			UploadStrategy: "document",
		},
		{
			Bucket:         "photos",
			Key:            "a/two.txt",
			Size:           2,
			ContentType:    "text/plain",
			ETag:           "etag-2",
			SHA256:         "sha-2",
			LastModified:   time.Unix(2, 0),
			ChunkCount:     1,
			TelegramType:   "document",
			UploadStrategy: "document",
		},
		{
			Bucket:         "photos",
			Key:            "a/zero.txt",
			Size:           0,
			ContentType:    "text/plain",
			ETag:           "etag-0",
			SHA256:         "sha-0",
			LastModified:   time.Unix(0, 0),
			ChunkCount:     1,
			TelegramType:   "document",
			UploadStrategy: "document",
		},
		{
			Bucket:         "photos",
			Key:            "b/three.txt",
			Size:           3,
			ContentType:    "text/plain",
			ETag:           "etag-3",
			SHA256:         "sha-3",
			LastModified:   time.Unix(3, 0),
			ChunkCount:     1,
			TelegramType:   "document",
			UploadStrategy: "document",
		},
	}
	for _, object := range objects {
		if err := store.PutObject(t.Context(), object, []Chunk{singleChunk(object.Bucket, object.Key)}); err != nil {
			t.Fatalf("PutObject(%q) returned error: %v", object.Key, err)
		}
	}

	ordered, err := store.ListObjects(t.Context(), ListQuery{Bucket: "photos", Prefix: "a/", Limit: 10})
	if err != nil {
		t.Fatalf("ListObjects ordered returned error: %v", err)
	}
	wantOrdered := []string{"a/one.txt", "a/two.txt", "a/zero.txt"}
	var gotOrdered []string
	for _, object := range ordered {
		gotOrdered = append(gotOrdered, object.Key)
	}
	if !reflect.DeepEqual(gotOrdered, wantOrdered) {
		t.Fatalf("ordered keys = %v, want %v", gotOrdered, wantOrdered)
	}

	limited, err := store.ListObjects(t.Context(), ListQuery{Bucket: "photos", Prefix: "a/", Limit: 2})
	if err != nil {
		t.Fatalf("ListObjects limited returned error: %v", err)
	}
	wantLimited := []string{"a/one.txt", "a/two.txt"}
	var gotLimited []string
	for _, object := range limited {
		gotLimited = append(gotLimited, object.Key)
	}
	if !reflect.DeepEqual(gotLimited, wantLimited) {
		t.Fatalf("limited keys = %v, want %v", gotLimited, wantLimited)
	}

	afterKey, err := store.ListObjects(t.Context(), ListQuery{Bucket: "photos", Prefix: "a/", AfterKey: "a/one.txt", Limit: 10})
	if err != nil {
		t.Fatalf("ListObjects after key returned error: %v", err)
	}
	wantAfterKey := []string{"a/two.txt", "a/zero.txt"}
	var gotAfterKey []string
	for _, object := range afterKey {
		gotAfterKey = append(gotAfterKey, object.Key)
	}
	if !reflect.DeepEqual(gotAfterKey, wantAfterKey) {
		t.Fatalf("after key keys = %v, want %v", gotAfterKey, wantAfterKey)
	}
}

func TestSQLiteListObjectsTreatsLikeWildcardsLiterally(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	objects := []Object{
		newTestObject("photos", "literal%/match.txt", 8),
		newTestObject("photos", "literal_/match.txt", 9),
		newTestObject("photos", "literalx/match.txt", 10),
		newTestObject("photos", "literaly/match.txt", 11),
	}
	for _, object := range objects {
		if err := store.PutObject(t.Context(), object, []Chunk{singleChunk(object.Bucket, object.Key)}); err != nil {
			t.Fatalf("PutObject(%q) returned error: %v", object.Key, err)
		}
	}

	percentObjects, err := store.ListObjects(t.Context(), ListQuery{Bucket: "photos", Prefix: "literal%/", Limit: 10})
	if err != nil {
		t.Fatalf("ListObjects with percent prefix returned error: %v", err)
	}
	assertObjectKeys(t, percentObjects, []string{"literal%/match.txt"})

	underscoreObjects, err := store.ListObjects(t.Context(), ListQuery{Bucket: "photos", Prefix: "literal_/", Limit: 10})
	if err != nil {
		t.Fatalf("ListObjects with underscore prefix returned error: %v", err)
	}
	assertObjectKeys(t, underscoreObjects, []string{"literal_/match.txt"})
}

func TestSQLiteDeleteAndMissingObject(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	if err := store.DeleteObject(t.Context(), "photos", "missing"); err != nil {
		t.Fatalf("DeleteObject returned error: %v", err)
	}

	_, _, err := store.GetObject(t.Context(), "photos", "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetObject error = %v, want ErrNotFound", err)
	}

	object := Object{
		Bucket:         "photos",
		Key:            "gone.txt",
		Size:           1,
		ContentType:    "text/plain",
		ETag:           "etag",
		SHA256:         "sha",
		LastModified:   time.Unix(4, 0),
		ChunkCount:     1,
		TelegramType:   "document",
		UploadStrategy: "document",
	}
	if err := store.PutObject(t.Context(), object, []Chunk{singleChunk("photos", "gone.txt")}); err != nil {
		t.Fatalf("PutObject returned error: %v", err)
	}
	if err := store.DeleteObject(t.Context(), "photos", "gone.txt"); err != nil {
		t.Fatalf("DeleteObject returned error: %v", err)
	}
	_, _, err = store.GetObject(t.Context(), "photos", "gone.txt")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetObject after delete error = %v, want ErrNotFound", err)
	}

	_, err = store.HeadObject(t.Context(), "photos", "gone.txt")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("HeadObject after delete error = %v, want ErrNotFound", err)
	}
}

func TestSQLiteListBucketsSkipsDisabled(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	if err := store.UpsertBucket(t.Context(), Bucket{Name: "beta", ChatID: "-2", CreatedAt: time.Unix(2, 0), Enabled: true}); err != nil {
		t.Fatalf("UpsertBucket beta returned error: %v", err)
	}
	if err := store.UpsertBucket(t.Context(), Bucket{Name: "alpha", ChatID: "-1", CreatedAt: time.Unix(1, 0), Enabled: false}); err != nil {
		t.Fatalf("UpsertBucket alpha returned error: %v", err)
	}

	got, err := store.ListBuckets(t.Context())
	if err != nil {
		t.Fatalf("ListBuckets returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(buckets) = %d", len(got))
	}
	if got[0].Name != "beta" {
		t.Fatalf("bucket name = %q", got[0].Name)
	}
}

func TestOpenSQLiteReadOnlyOpensExistingRelativeDatabase(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.Mkdir("data", 0o755); err != nil {
		t.Fatalf("Mkdir returned error: %v", err)
	}
	path := filepath.Join("data", "metadata.sqlite")
	writable, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	if err := writable.UpsertBucket(t.Context(), Bucket{Name: "photos", ChatID: "-100", CreatedAt: time.Unix(10, 0), Enabled: true}); err != nil {
		t.Fatalf("UpsertBucket returned error: %v", err)
	}
	if err := writable.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	readonly, err := OpenSQLiteReadOnly(path)
	if err != nil {
		t.Fatalf("OpenSQLiteReadOnly returned error: %v", err)
	}
	defer readonly.Close()

	buckets, err := readonly.ListBuckets(t.Context())
	if err != nil {
		t.Fatalf("ListBuckets returned error: %v", err)
	}
	if len(buckets) != 1 || buckets[0].Name != "photos" {
		t.Fatalf("buckets = %+v", buckets)
	}
}

func TestOpenSQLiteReadOnlyOpensExistingDatabaseWithoutWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	writable, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	if err := writable.UpsertBucket(t.Context(), Bucket{Name: "photos", ChatID: "-100", CreatedAt: time.Unix(10, 0), Enabled: true}); err != nil {
		t.Fatalf("UpsertBucket returned error: %v", err)
	}
	if err := writable.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	readonly, err := OpenSQLiteReadOnly(path)
	if err != nil {
		t.Fatalf("OpenSQLiteReadOnly returned error: %v", err)
	}
	defer readonly.Close()

	buckets, err := readonly.ListBuckets(t.Context())
	if err != nil {
		t.Fatalf("ListBuckets returned error: %v", err)
	}
	if len(buckets) != 1 || buckets[0].Name != "photos" {
		t.Fatalf("buckets = %+v", buckets)
	}
	if err := readonly.UpsertBucket(t.Context(), Bucket{Name: "backups", ChatID: "-200", CreatedAt: time.Unix(20, 0), Enabled: true}); err == nil {
		t.Fatal("expected read-only UpsertBucket error")
	}
}

func TestOpenSQLiteReadOnlyDoesNotCreateMissingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.sqlite")
	store, err := OpenSQLiteReadOnly(path)
	if err == nil {
		_ = store.Close()
		t.Fatal("expected OpenSQLiteReadOnly error")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("sqlite path was created or stat failed unexpectedly: %v", statErr)
	}
}

func TestSQLiteCopyObjectCreatesNew(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	src := newTestObject("photos", "src.txt", 10)
	src.Size = 100
	src.ETag = "etag1"
	chunks := []Chunk{singleChunk("photos", "src.txt")}
	if err := store.PutObject(t.Context(), src, chunks); err != nil {
		t.Fatalf("PutObject source returned error: %v", err)
	}

	result, err := store.CopyObject(t.Context(), "photos", "src.txt", "dst.txt", CopyOptions{})
	if err != nil {
		t.Fatalf("CopyObject returned error: %v", err)
	}
	if !result.Created {
		t.Fatal("Created = false, want true")
	}

	gotObject, gotChunks, err := store.GetObject(t.Context(), "photos", "dst.txt")
	if err != nil {
		t.Fatalf("GetObject destination returned error: %v", err)
	}
	if gotObject.Key != "dst.txt" || gotObject.Size != 100 || gotObject.ETag != "etag1" || gotObject.SHA256 != src.SHA256 {
		t.Fatalf("destination object = %+v", gotObject)
	}
	if len(gotChunks) != 1 || gotChunks[0].Key != "dst.txt" || gotChunks[0].TelegramFileID != chunks[0].TelegramFileID {
		t.Fatalf("destination chunks = %+v", gotChunks)
	}
}

func TestSQLiteCopyObjectNoOverwriteFailsIfExists(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	if err := store.PutObject(t.Context(), newTestObject("photos", "src.txt", 10), []Chunk{singleChunk("photos", "src.txt")}); err != nil {
		t.Fatalf("PutObject source returned error: %v", err)
	}
	if err := store.PutObject(t.Context(), newTestObject("photos", "dst.txt", 20), []Chunk{singleChunk("photos", "dst.txt")}); err != nil {
		t.Fatalf("PutObject destination returned error: %v", err)
	}

	_, err := store.CopyObject(t.Context(), "photos", "src.txt", "dst.txt", CopyOptions{Overwrite: false})
	if err == nil {
		t.Fatal("CopyObject returned nil error, want destination exists error")
	}

	gotObject, _, err := store.GetObject(t.Context(), "photos", "dst.txt")
	if err != nil {
		t.Fatalf("GetObject destination returned error: %v", err)
	}
	if gotObject.ETag != "dst.txt-etag" {
		t.Fatalf("destination was modified: %+v", gotObject)
	}
}

func TestSQLiteCopyObjectOverwriteReplacesDestination(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	src := newTestObject("photos", "src.txt", 10)
	src.Size = 100
	src.ETag = "etag1"
	if err := store.PutObject(t.Context(), src, []Chunk{singleChunk("photos", "src.txt")}); err != nil {
		t.Fatalf("PutObject source returned error: %v", err)
	}
	if err := store.PutObject(t.Context(), newTestObject("photos", "dst.txt", 20), []Chunk{singleChunk("photos", "dst.txt")}); err != nil {
		t.Fatalf("PutObject destination returned error: %v", err)
	}

	result, err := store.CopyObject(t.Context(), "photos", "src.txt", "dst.txt", CopyOptions{Overwrite: true})
	if err != nil {
		t.Fatalf("CopyObject returned error: %v", err)
	}
	if result.Created {
		t.Fatal("Created = true, want false for overwrite")
	}

	gotObject, _, err := store.GetObject(t.Context(), "photos", "dst.txt")
	if err != nil {
		t.Fatalf("GetObject destination returned error: %v", err)
	}
	if gotObject.Size != 100 || gotObject.ETag != "etag1" {
		t.Fatalf("destination object = %+v", gotObject)
	}
}

func TestSQLiteMoveObjectSamePathPreservesSource(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	src := newTestObject("photos", "same.txt", 10)
	src.Size = 100
	if err := store.PutObject(t.Context(), src, []Chunk{singleChunk("photos", "same.txt")}); err != nil {
		t.Fatalf("PutObject source returned error: %v", err)
	}

	_, err := store.MoveObject(t.Context(), "photos", "same.txt", "same.txt", MoveOptions{Overwrite: true})
	if err == nil {
		t.Fatal("expected same-path move error")
	}
	if _, _, err := store.GetObject(t.Context(), "photos", "same.txt"); err != nil {
		t.Fatalf("source missing after rejected same-path move: %v", err)
	}
}

func TestSQLiteMoveObjectCreatesNewAndDeletesSource(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	src := newTestObject("photos", "src.txt", 10)
	src.Size = 100
	src.ETag = "etag1"
	if err := store.PutObject(t.Context(), src, []Chunk{singleChunk("photos", "src.txt")}); err != nil {
		t.Fatalf("PutObject source returned error: %v", err)
	}

	result, err := store.MoveObject(t.Context(), "photos", "src.txt", "dst.txt", MoveOptions{})
	if err != nil {
		t.Fatalf("MoveObject returned error: %v", err)
	}
	if !result.Created {
		t.Fatal("Created = false, want true")
	}

	_, err = store.HeadObject(t.Context(), "photos", "src.txt")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("HeadObject source error = %v, want ErrNotFound", err)
	}
	gotObject, gotChunks, err := store.GetObject(t.Context(), "photos", "dst.txt")
	if err != nil {
		t.Fatalf("GetObject destination returned error: %v", err)
	}
	if gotObject.Size != 100 || gotObject.ETag != "etag1" {
		t.Fatalf("destination object = %+v", gotObject)
	}
	if len(gotChunks) != 1 || gotChunks[0].Key != "dst.txt" {
		t.Fatalf("destination chunks = %+v", gotChunks)
	}
}

func TestSQLiteCopyPrefixRecursive(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	for _, object := range []Object{
		newTestObject("photos", "dir/a.txt", 10),
		newTestObject("photos", "dir/b.txt", 20),
		newTestObject("photos", "dir/sub/c.txt", 30),
		newTestObject("photos", "dir2/sibling.txt", 40),
	} {
		if err := store.PutObject(t.Context(), object, []Chunk{singleChunk(object.Bucket, object.Key)}); err != nil {
			t.Fatalf("PutObject(%q) returned error: %v", object.Key, err)
		}
	}

	_, err := store.CopyPrefix(t.Context(), "photos", "dir/", "copy/", CopyOptions{})
	if err != nil {
		t.Fatalf("CopyPrefix returned error: %v", err)
	}

	copied, err := store.ListAllObjects(t.Context(), "photos", "copy/")
	if err != nil {
		t.Fatalf("ListAllObjects copy returned error: %v", err)
	}
	assertObjectKeys(t, copied, []string{"copy/a.txt", "copy/b.txt", "copy/sub/c.txt"})
	originals, err := store.ListAllObjects(t.Context(), "photos", "dir/")
	if err != nil {
		t.Fatalf("ListAllObjects original returned error: %v", err)
	}
	assertObjectKeys(t, originals, []string{"dir/a.txt", "dir/b.txt", "dir/sub/c.txt"})
}

func TestSQLiteMovePrefixSamePathPreservesSource(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	for _, object := range []Object{
		newTestObject("photos", "dir/a.txt", 10),
		newTestObject("photos", "dir/b.txt", 20),
	} {
		if err := store.PutObject(t.Context(), object, []Chunk{singleChunk(object.Bucket, object.Key)}); err != nil {
			t.Fatalf("PutObject(%q) returned error: %v", object.Key, err)
		}
	}

	_, err := store.MovePrefix(t.Context(), "photos", "dir/", "dir/", MoveOptions{Overwrite: true})
	if err == nil {
		t.Fatal("expected same-prefix move error")
	}
	objects, err := store.ListAllObjects(t.Context(), "photos", "dir/")
	if err != nil {
		t.Fatalf("ListAllObjects returned error: %v", err)
	}
	assertObjectKeys(t, objects, []string{"dir/a.txt", "dir/b.txt"})
}

func TestSQLiteMovePrefixRecursive(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	for _, object := range []Object{
		newTestObject("photos", "dir/a.txt", 10),
		newTestObject("photos", "dir/b.txt", 20),
	} {
		if err := store.PutObject(t.Context(), object, []Chunk{singleChunk(object.Bucket, object.Key)}); err != nil {
			t.Fatalf("PutObject(%q) returned error: %v", object.Key, err)
		}
	}

	_, err := store.MovePrefix(t.Context(), "photos", "dir/", "moved/", MoveOptions{})
	if err != nil {
		t.Fatalf("MovePrefix returned error: %v", err)
	}

	originals, err := store.ListAllObjects(t.Context(), "photos", "dir/")
	if err != nil {
		t.Fatalf("ListAllObjects original returned error: %v", err)
	}
	if len(originals) != 0 {
		t.Fatalf("source objects = %+v, want none", originals)
	}
	moved, err := store.ListAllObjects(t.Context(), "photos", "moved/")
	if err != nil {
		t.Fatalf("ListAllObjects moved returned error: %v", err)
	}
	assertObjectKeys(t, moved, []string{"moved/a.txt", "moved/b.txt"})
}

func TestSQLiteDeletePrefixRecursive(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	for _, object := range []Object{
		newTestObject("photos", "dir/a.txt", 10),
		newTestObject("photos", "dir/b.txt", 20),
		newTestObject("photos", "other.txt", 30),
	} {
		if err := store.PutObject(t.Context(), object, []Chunk{singleChunk(object.Bucket, object.Key)}); err != nil {
			t.Fatalf("PutObject(%q) returned error: %v", object.Key, err)
		}
	}

	if err := store.DeletePrefix(t.Context(), "photos", "dir/"); err != nil {
		t.Fatalf("DeletePrefix returned error: %v", err)
	}

	underDir, err := store.ListAllObjects(t.Context(), "photos", "dir/")
	if err != nil {
		t.Fatalf("ListAllObjects dir returned error: %v", err)
	}
	if len(underDir) != 0 {
		t.Fatalf("dir objects = %+v, want none", underDir)
	}
	remaining, err := store.ListAllObjects(t.Context(), "photos", "")
	if err != nil {
		t.Fatalf("ListAllObjects all returned error: %v", err)
	}
	assertObjectKeys(t, remaining, []string{"other.txt"})
}

func TestSQLiteDeleteBucketRemovesBucketObjectsAndChunks(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	if err := store.UpsertBucket(t.Context(), Bucket{Name: "photos", ChatID: "-100", CreatedAt: time.Unix(1, 0), Enabled: false}); err != nil {
		t.Fatalf("UpsertBucket returned error: %v", err)
	}
	if err := store.PutObject(t.Context(), newTestObject("photos", "a.txt", 10), []Chunk{singleChunk("photos", "a.txt")}); err != nil {
		t.Fatalf("PutObject returned error: %v", err)
	}

	if err := store.DeleteBucket(t.Context(), "photos"); err != nil {
		t.Fatalf("DeleteBucket returned error: %v", err)
	}

	_, err := store.GetBucket(t.Context(), "photos")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetBucket error = %v, want ErrNotFound", err)
	}
	objects, err := store.ListAllObjects(t.Context(), "photos", "")
	if err != nil {
		t.Fatalf("ListAllObjects returned error: %v", err)
	}
	if len(objects) != 0 {
		t.Fatalf("objects = %+v, want none", objects)
	}
}

func TestSQLiteDisableBucketsExceptDisablesRemovedBuckets(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	for _, b := range []Bucket{
		{Name: "photos", ChatID: "-100", CreatedAt: time.Unix(1, 0), Enabled: true},
		{Name: "archive", ChatID: "-200", CreatedAt: time.Unix(2, 0), Enabled: true},
		{Name: "backups", ChatID: "-300", CreatedAt: time.Unix(3, 0), Enabled: true},
	} {
		if err := store.UpsertBucket(t.Context(), b); err != nil {
			t.Fatalf("UpsertBucket(%s) returned error: %v", b.Name, err)
		}
	}

	if err := store.DisableBucketsExcept(t.Context(), []string{"photos", "backups"}); err != nil {
		t.Fatalf("DisableBucketsExcept returned error: %v", err)
	}

	photos, err := store.GetBucket(t.Context(), "photos")
	if err != nil {
		t.Fatalf("GetBucket photos returned error: %v", err)
	}
	if !photos.Enabled {
		t.Fatal("photos should remain enabled")
	}
	backups, err := store.GetBucket(t.Context(), "backups")
	if err != nil {
		t.Fatalf("GetBucket backups returned error: %v", err)
	}
	if !backups.Enabled {
		t.Fatal("backups should remain enabled")
	}
	archive, err := store.GetBucket(t.Context(), "archive")
	if err != nil {
		t.Fatalf("GetBucket archive returned error: %v", err)
	}
	if archive.Enabled {
		t.Fatal("archive should be disabled")
	}
}

func TestSQLiteDisableBucketsExceptEmptyKeepListDisablesAll(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	if err := store.UpsertBucket(t.Context(), Bucket{Name: "photos", ChatID: "-100", CreatedAt: time.Unix(1, 0), Enabled: true}); err != nil {
		t.Fatalf("UpsertBucket returned error: %v", err)
	}
	if err := store.DisableBucketsExcept(t.Context(), nil); err != nil {
		t.Fatalf("DisableBucketsExcept returned error: %v", err)
	}
	bucket, err := store.GetBucket(t.Context(), "photos")
	if err != nil {
		t.Fatalf("GetBucket returned error: %v", err)
	}
	if bucket.Enabled {
		t.Fatal("photos should be disabled when keep list is empty")
	}
}

func TestSQLiteDisableBucketsExceptNoopWhenAllKept(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()

	if err := store.UpsertBucket(t.Context(), Bucket{Name: "photos", ChatID: "-100", CreatedAt: time.Unix(1, 0), Enabled: true}); err != nil {
		t.Fatalf("UpsertBucket returned error: %v", err)
	}
	if err := store.DisableBucketsExcept(t.Context(), []string{"photos"}); err != nil {
		t.Fatalf("DisableBucketsExcept returned error: %v", err)
	}
	bucket, err := store.GetBucket(t.Context(), "photos")
	if err != nil {
		t.Fatalf("GetBucket returned error: %v", err)
	}
	if !bucket.Enabled {
		t.Fatal("photos should remain enabled")
	}
}

func TestSQLiteCountBucketRenameRowsDoesNotModifyData(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()
	ctx := t.Context()

	createdAt := time.Unix(1779235200, 0)
	if err := store.UpsertBucket(ctx, Bucket{Name: "old", ChatID: "-100111", CreatedAt: createdAt, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutObject(ctx, Object{Bucket: "old", Key: "a.txt", Size: 3, ContentType: "text/plain", ETag: "etag1", SHA256: "sha1", LastModified: createdAt, ChunkCount: 1, TelegramType: "document", UploadStrategy: "single"}, []Chunk{{Bucket: "old", Key: "a.txt", PartNumber: 1, Offset: 0, Size: 3, TelegramType: "document", TelegramFileID: "f1", SHA256: "csha1"}}); err != nil {
		t.Fatal(err)
	}

	rename, err := store.CountBucketRenameRows(ctx, "old")
	if err != nil {
		t.Fatal(err)
	}
	if rename.Buckets != 1 || rename.Objects != 1 || rename.Chunks != 1 {
		t.Fatalf("unexpected counts: %+v", rename)
	}

	found, err := store.GetBucket(ctx, "old")
	if err != nil {
		t.Fatal(err)
	}
	if found.ChatID != "-100111" {
		t.Fatalf("bucket modified by count: %+v", found)
	}
}

func TestSQLiteRenameBucketRenamesBucketObjectsAndChunks(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()
	ctx := t.Context()

	createdAt := time.Unix(1779235200, 0)
	if err := store.UpsertBucket(ctx, Bucket{Name: "old", ChatID: "-100222", CreatedAt: createdAt, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutObject(ctx, Object{Bucket: "old", Key: "doc.txt", Size: 5, ContentType: "text/plain", ETag: "etag2", SHA256: "sha2", LastModified: createdAt, ChunkCount: 1, TelegramType: "document", UploadStrategy: "single"}, []Chunk{{Bucket: "old", Key: "doc.txt", PartNumber: 1, Offset: 0, Size: 5, TelegramType: "document", TelegramFileID: "f2", SHA256: "csha2"}}); err != nil {
		t.Fatal(err)
	}

	rename, err := store.RenameBucket(ctx, "old", "new")
	if err != nil {
		t.Fatal(err)
	}
	if rename.Buckets != 1 || rename.Objects != 1 || rename.Chunks != 1 {
		t.Fatalf("unexpected counts: %+v", rename)
	}

	_, err = store.GetBucket(ctx, "old")
	if err != ErrNotFound {
		t.Fatalf("expected old bucket gone, got err=%v", err)
	}

	bucket, err := store.GetBucket(ctx, "new")
	if err != nil {
		t.Fatal(err)
	}
	if bucket.ChatID != "-100222" || bucket.CreatedAt != createdAt {
		t.Fatalf("bucket metadata not preserved: %+v", bucket)
	}

	obj, chunks, err := store.GetObject(ctx, "new", "doc.txt")
	if err != nil {
		t.Fatal(err)
	}
	if obj.Bucket != "new" || obj.ETag != "etag2" {
		t.Fatalf("object not renamed: %+v", obj)
	}
	if len(chunks) != 1 || chunks[0].Bucket != "new" || chunks[0].TelegramFileID != "f2" {
		t.Fatalf("chunks not renamed: %+v", chunks)
	}
}

func TestSQLiteRenameBucketRejectsExistingTarget(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()
	ctx := t.Context()

	createdAt := time.Unix(1779235200, 0)
	if err := store.UpsertBucket(ctx, Bucket{Name: "old", ChatID: "-100333", CreatedAt: createdAt, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBucket(ctx, Bucket{Name: "new", ChatID: "-100444", CreatedAt: createdAt, Enabled: true}); err != nil {
		t.Fatal(err)
	}

	_, err := store.RenameBucket(ctx, "old", "new")
	if err == nil {
		t.Fatal("expected error renaming to existing bucket")
	}

	bucket, err := store.GetBucket(ctx, "old")
	if err != nil {
		t.Fatal(err)
	}
	if bucket.Name != "old" {
		t.Fatalf("old bucket should be unchanged: %+v", bucket)
	}
}

func TestSQLiteRenameBucketPreservesChatID(t *testing.T) {
	store := openTestSQLiteStore(t)
	defer store.Close()
	ctx := t.Context()

	createdAt := time.Unix(1779235200, 0)
	if err := store.UpsertBucket(ctx, Bucket{Name: "old", ChatID: "-100999", CreatedAt: createdAt, Enabled: true}); err != nil {
		t.Fatal(err)
	}

	_, err := store.RenameBucket(ctx, "old", "new")
	if err != nil {
		t.Fatal(err)
	}

	bucket, err := store.GetBucket(ctx, "new")
	if err != nil {
		t.Fatal(err)
	}
	if bucket.ChatID != "-100999" {
		t.Fatalf("chat_id not preserved: %s", bucket.ChatID)
	}
}

func openTestSQLiteStore(t *testing.T) *SQLiteStore {
	t.Helper()

	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	store, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	return store
}

func newTestObject(bucket, key string, unixTime int64) Object {
	return Object{
		Bucket:         bucket,
		Key:            key,
		Size:           1,
		ContentType:    "text/plain",
		ETag:           key + "-etag",
		SHA256:         key + "-sha",
		LastModified:   time.Unix(unixTime, 0),
		ChunkCount:     1,
		TelegramType:   "document",
		UploadStrategy: "document",
	}
}

func assertObjectKeys(t *testing.T, objects []Object, want []string) {
	t.Helper()

	got := make([]string, 0, len(objects))
	for _, object := range objects {
		got = append(got, object.Key)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
}

func singleChunk(bucket, key string) Chunk {
	return Chunk{
		Bucket:               bucket,
		Key:                  key,
		PartNumber:           1,
		Offset:               0,
		Size:                 1,
		TelegramType:         "document",
		TelegramFileID:       key + "-file",
		TelegramMessageID:    1,
		TelegramFileUniqueID: key + "-unique",
		SHA256:               key + "-sha",
	}
}
