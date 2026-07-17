package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
}

func OpenSQLite(path string) (*SQLiteStore, error) {
	store, err := openSQLite(path)
	if err != nil {
		return nil, err
	}
	if err := store.migrate(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func OpenSQLiteReadOnly(path string) (*SQLiteStore, error) {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	return openSQLite(sqliteReadOnlyDSN(absolutePath))
}

func openSQLite(dataSourceName string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dataSourceName)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SQLiteStore{db: db}, nil
}

func sqliteReadOnlyDSN(path string) string {
	// On Windows, filepath.Abs returns backslashes which break file:// URLs.
	// Convert to forward slashes and prepend / so url.URL produces file:///path
	// (three slashes) which modernc/sqlite expects on Windows.
	cleanPath := "/" + strings.ReplaceAll(path, "\\", "/")
	u := url.URL{Scheme: "file", Path: cleanPath}
	query := u.Query()
	query.Set("mode", "ro")
	u.RawQuery = query.Encode()
	return u.String()
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) UpsertBucket(ctx context.Context, bucket Bucket) error {
	_, err := s.db.ExecContext(ctx, `
	INSERT INTO buckets (name, chat_id, created_at, enabled)
	VALUES (?, ?, ?, ?)
	ON CONFLICT(name) DO UPDATE SET
		chat_id = excluded.chat_id,
		created_at = excluded.created_at,
		enabled = excluded.enabled
	`, bucket.Name, bucket.ChatID, bucket.CreatedAt.Unix(), boolToInt(bucket.Enabled))
	return err
}

func (s *SQLiteStore) GetBucket(ctx context.Context, name string) (Bucket, error) {
	row := s.db.QueryRowContext(ctx, `
	SELECT name, chat_id, created_at, enabled
	FROM buckets
	WHERE name = ?
	`, name)

	var bucket Bucket
	var createdAt int64
	var enabled int
	if err := row.Scan(&bucket.Name, &bucket.ChatID, &createdAt, &enabled); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Bucket{}, ErrNotFound
		}
		return Bucket{}, err
	}

	bucket.CreatedAt = unixSeconds(createdAt)
	bucket.Enabled = enabled != 0
	return bucket, nil
}

func (s *SQLiteStore) ListBuckets(ctx context.Context) ([]Bucket, error) {
	rows, err := s.db.QueryContext(ctx, `
	SELECT name, chat_id, created_at, enabled
	FROM buckets
	WHERE enabled = 1
	ORDER BY name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var buckets []Bucket
	for rows.Next() {
		var bucket Bucket
		var createdAt int64
		var enabled int
		if err := rows.Scan(&bucket.Name, &bucket.ChatID, &createdAt, &enabled); err != nil {
			return nil, err
		}
		bucket.CreatedAt = unixSeconds(createdAt)
		bucket.Enabled = enabled != 0
		buckets = append(buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return buckets, nil
}

func (s *SQLiteStore) PutObject(ctx context.Context, object Object, chunks []Chunk) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE bucket = ? AND key = ?`, object.Bucket, object.Key); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
	INSERT INTO objects (bucket, key, size, content_type, etag, sha256, last_modified, chunk_count, telegram_type, upload_strategy)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(bucket, key) DO UPDATE SET
		size = excluded.size,
		content_type = excluded.content_type,
		etag = excluded.etag,
		sha256 = excluded.sha256,
		last_modified = excluded.last_modified,
		chunk_count = excluded.chunk_count,
		telegram_type = excluded.telegram_type,
		upload_strategy = excluded.upload_strategy
	`, object.Bucket, object.Key, object.Size, object.ContentType, object.ETag, object.SHA256, object.LastModified.Unix(), object.ChunkCount, object.TelegramType, object.UploadStrategy); err != nil {
		return err
	}

	for _, chunk := range chunks {
		if _, err := tx.ExecContext(ctx, `
	INSERT INTO object_chunks (bucket, key, part_number, offset, size, telegram_type, telegram_file_id, telegram_message_id, telegram_file_unique_id, sha256)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, chunk.Bucket, chunk.Key, chunk.PartNumber, chunk.Offset, chunk.Size, chunk.TelegramType, chunk.TelegramFileID, chunk.TelegramMessageID, chunk.TelegramFileUniqueID, chunk.SHA256); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) GetObject(ctx context.Context, bucket, key string) (Object, []Chunk, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Object{}, nil, err
	}
	defer tx.Rollback()

	object, err := scanObject(tx.QueryRowContext(ctx, `
	SELECT bucket, key, size, content_type, etag, sha256, last_modified, chunk_count, telegram_type, upload_strategy
	FROM objects
	WHERE bucket = ? AND key = ?
	`, bucket, key))
	if err != nil {
		return Object{}, nil, err
	}

	rows, err := tx.QueryContext(ctx, `
	SELECT bucket, key, part_number, offset, size, telegram_type, telegram_file_id, telegram_message_id, telegram_file_unique_id, sha256
	FROM object_chunks
	WHERE bucket = ? AND key = ?
	ORDER BY part_number ASC
	`, bucket, key)
	if err != nil {
		return Object{}, nil, err
	}
	defer rows.Close()

	var chunks []Chunk
	for rows.Next() {
		var chunk Chunk
		if err := rows.Scan(&chunk.Bucket, &chunk.Key, &chunk.PartNumber, &chunk.Offset, &chunk.Size, &chunk.TelegramType, &chunk.TelegramFileID, &chunk.TelegramMessageID, &chunk.TelegramFileUniqueID, &chunk.SHA256); err != nil {
			return Object{}, nil, err
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return Object{}, nil, err
	}
	if err := tx.Commit(); err != nil {
		return Object{}, nil, err
	}

	return object, chunks, nil
}

func (s *SQLiteStore) HeadObject(ctx context.Context, bucket, key string) (Object, error) {
	object, err := scanObject(s.db.QueryRowContext(ctx, `
	SELECT bucket, key, size, content_type, etag, sha256, last_modified, chunk_count, telegram_type, upload_strategy
	FROM objects
	WHERE bucket = ? AND key = ?
	`, bucket, key))
	if err != nil {
		return Object{}, err
	}
	return object, nil
}

func (s *SQLiteStore) ListObjects(ctx context.Context, query ListQuery) ([]Object, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = 1000
	}

	rows, err := s.db.QueryContext(ctx, `
	SELECT bucket, key, size, content_type, etag, sha256, last_modified, chunk_count, telegram_type, upload_strategy
	FROM objects
	WHERE bucket = ?
	  AND key LIKE ? ESCAPE '\'
	  AND key > ?
	ORDER BY key ASC
	LIMIT ?
	`, query.Bucket, escapeSQLiteLikePattern(query.Prefix)+"%", query.AfterKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []Object
	for rows.Next() {
		object, err := scanObject(rows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return objects, nil
}

func (s *SQLiteStore) DeleteObject(ctx context.Context, bucket, key string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *SQLiteStore) CopyObject(ctx context.Context, bucket, srcKey, dstKey string, options CopyOptions) (CopyResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CopyResult{}, err
	}
	defer tx.Rollback()

	src, chunks, err := getObjectInTx(ctx, tx, bucket, srcKey)
	if err != nil {
		return CopyResult{}, err
	}

	dstExists, err := objectExistsInTx(ctx, tx, bucket, dstKey)
	if err != nil {
		return CopyResult{}, err
	}
	if dstExists && !options.Overwrite {
		return CopyResult{}, errors.New("destination already exists")
	}

	if err := putObjectInTx(ctx, tx, Object{
		Bucket:         bucket,
		Key:            dstKey,
		Size:           src.Size,
		ContentType:    src.ContentType,
		ETag:           src.ETag,
		SHA256:         src.SHA256,
		LastModified:   time.Now().UTC(),
		ChunkCount:     src.ChunkCount,
		TelegramType:   src.TelegramType,
		UploadStrategy: src.UploadStrategy,
	}, copyChunksForKey(chunks, bucket, dstKey)); err != nil {
		return CopyResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return CopyResult{}, err
	}
	return CopyResult{Created: !dstExists}, nil
}

func (s *SQLiteStore) MoveObject(ctx context.Context, bucket, srcKey, dstKey string, options MoveOptions) (MoveResult, error) {
	if srcKey == dstKey {
		return MoveResult{}, errors.New("source and destination are the same")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MoveResult{}, err
	}
	defer tx.Rollback()

	src, chunks, err := getObjectInTx(ctx, tx, bucket, srcKey)
	if err != nil {
		return MoveResult{}, err
	}

	dstExists, err := objectExistsInTx(ctx, tx, bucket, dstKey)
	if err != nil {
		return MoveResult{}, err
	}
	if dstExists && !options.Overwrite {
		return MoveResult{}, errors.New("destination already exists")
	}

	if err := putObjectInTx(ctx, tx, Object{
		Bucket:         bucket,
		Key:            dstKey,
		Size:           src.Size,
		ContentType:    src.ContentType,
		ETag:           src.ETag,
		SHA256:         src.SHA256,
		LastModified:   time.Now().UTC(),
		ChunkCount:     src.ChunkCount,
		TelegramType:   src.TelegramType,
		UploadStrategy: src.UploadStrategy,
	}, copyChunksForKey(chunks, bucket, dstKey)); err != nil {
		return MoveResult{}, err
	}

	if err := deleteObjectInTx(ctx, tx, bucket, srcKey); err != nil {
		return MoveResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return MoveResult{}, err
	}
	return MoveResult{Created: !dstExists}, nil
}

func (s *SQLiteStore) CopyPrefix(ctx context.Context, bucket, srcPrefix, dstPrefix string, options CopyOptions) (CopyResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CopyResult{}, err
	}
	defer tx.Rollback()

	created, err := copyPrefixInTx(ctx, tx, bucket, srcPrefix, dstPrefix, options.Overwrite)
	if err != nil {
		return CopyResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return CopyResult{}, err
	}
	return CopyResult{Created: created}, nil
}

func (s *SQLiteStore) MovePrefix(ctx context.Context, bucket, srcPrefix, dstPrefix string, options MoveOptions) (MoveResult, error) {
	if srcPrefix == dstPrefix {
		return MoveResult{}, errors.New("source and destination are the same")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MoveResult{}, err
	}
	defer tx.Rollback()

	created, err := copyPrefixInTx(ctx, tx, bucket, srcPrefix, dstPrefix, options.Overwrite)
	if err != nil {
		return MoveResult{}, err
	}
	if err := deletePrefixInTx(ctx, tx, bucket, srcPrefix); err != nil {
		return MoveResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return MoveResult{}, err
	}
	return MoveResult{Created: created}, nil
}

func (s *SQLiteStore) DeletePrefix(ctx context.Context, bucket, prefix string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := deletePrefixInTx(ctx, tx, bucket, prefix); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) DeleteBucket(ctx context.Context, bucket string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE bucket = ?`, bucket); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE bucket = ?`, bucket); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, bucket); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) ListAllObjects(ctx context.Context, bucket, prefix string) ([]Object, error) {
	var all []Object
	afterKey := ""
	for {
		objects, err := s.ListObjects(ctx, ListQuery{Bucket: bucket, Prefix: prefix, AfterKey: afterKey, Limit: 1000})
		if err != nil {
			return nil, err
		}
		if len(objects) == 0 {
			return all, nil
		}
		all = append(all, objects...)
		afterKey = objects[len(objects)-1].Key
	}
}

func (s *SQLiteStore) CountObjects(ctx context.Context, bucket, prefix string) (int, error) {
	row := s.db.QueryRowContext(ctx, `
	SELECT COUNT(*)
	FROM objects
	WHERE bucket = ?
	  AND key LIKE ? ESCAPE '\'
	`, bucket, escapeSQLiteLikePattern(prefix)+"%")
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
func (s *SQLiteStore) DisableBucketsExcept(ctx context.Context, keepNames []string) error {
	if len(keepNames) == 0 {
		_, err := s.db.ExecContext(ctx, `UPDATE buckets SET enabled = 0 WHERE enabled = 1`)
		return err
	}
	query := `UPDATE buckets SET enabled = 0 WHERE enabled = 1 AND name NOT IN (?` + strings.Repeat(",?", len(keepNames)-1) + `)`
	args := make([]any, len(keepNames))
	for i, name := range keepNames {
		args[i] = name
	}
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *SQLiteStore) CountBucketRenameRows(ctx context.Context, oldName string) (BucketRename, error) {
	var counts BucketRename
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM buckets WHERE name = ?`, oldName).Scan(&counts.Buckets); err != nil {
		return BucketRename{}, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects WHERE bucket = ?`, oldName).Scan(&counts.Objects); err != nil {
		return BucketRename{}, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM object_chunks WHERE bucket = ?`, oldName).Scan(&counts.Chunks); err != nil {
		return BucketRename{}, err
	}
	return counts, nil
}

func (s *SQLiteStore) RenameBucket(ctx context.Context, oldName, newName string) (BucketRename, error) {
	if oldName == newName {
		return BucketRename{}, errors.New("source and destination bucket are the same")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return BucketRename{}, err
	}
	defer tx.Rollback()

	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM buckets WHERE name = ?`, oldName).Scan(&exists); err != nil {
		return BucketRename{}, err
	}
	if exists == 0 {
		return BucketRename{}, ErrNotFound
	}

	var targetExists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM buckets WHERE name = ?`, newName).Scan(&targetExists); err != nil {
		return BucketRename{}, err
	}
	if targetExists > 0 {
		return BucketRename{}, fmt.Errorf("destination bucket already exists: %s", newName)
	}

	var counts BucketRename
	counts.Buckets = 1
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects WHERE bucket = ?`, oldName).Scan(&counts.Objects); err != nil {
		return BucketRename{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM object_chunks WHERE bucket = ?`, oldName).Scan(&counts.Chunks); err != nil {
		return BucketRename{}, err
	}

	if _, err := tx.ExecContext(ctx, `UPDATE buckets SET name = ? WHERE name = ?`, newName, oldName); err != nil {
		return BucketRename{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET bucket = ? WHERE bucket = ?`, newName, oldName); err != nil {
		return BucketRename{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE object_chunks SET bucket = ? WHERE bucket = ?`, newName, oldName); err != nil {
		return BucketRename{}, err
	}

	return counts, tx.Commit()
}

func (s *SQLiteStore) migrate() error {
	_, err := s.db.Exec(`
	CREATE TABLE IF NOT EXISTS buckets (
		name TEXT PRIMARY KEY,
		chat_id TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		enabled INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS objects (
		bucket TEXT NOT NULL,
		key TEXT NOT NULL,
		size INTEGER NOT NULL,
		content_type TEXT NOT NULL,
		etag TEXT NOT NULL,
		sha256 TEXT NOT NULL,
		last_modified INTEGER NOT NULL,
		chunk_count INTEGER NOT NULL,
		telegram_type TEXT NOT NULL,
		upload_strategy TEXT NOT NULL,
		PRIMARY KEY(bucket, key)
	);

	CREATE TABLE IF NOT EXISTS object_chunks (
		bucket TEXT NOT NULL,
		key TEXT NOT NULL,
		part_number INTEGER NOT NULL,
		offset INTEGER NOT NULL,
		size INTEGER NOT NULL,
		telegram_type TEXT NOT NULL,
		telegram_file_id TEXT NOT NULL,
		telegram_message_id INTEGER NOT NULL,
		telegram_file_unique_id TEXT NOT NULL,
		sha256 TEXT NOT NULL,
		PRIMARY KEY(bucket, key, part_number)
	);
	`)
	return err
}

type objectScanner interface {
	Scan(dest ...any) error
}

func getObjectInTx(ctx context.Context, tx *sql.Tx, bucket, key string) (Object, []Chunk, error) {
	object, err := scanObject(tx.QueryRowContext(ctx, `
	SELECT bucket, key, size, content_type, etag, sha256, last_modified, chunk_count, telegram_type, upload_strategy
	FROM objects
	WHERE bucket = ? AND key = ?
	`, bucket, key))
	if err != nil {
		return Object{}, nil, err
	}
	chunks, err := listChunksInTx(ctx, tx, bucket, key)
	if err != nil {
		return Object{}, nil, err
	}
	return object, chunks, nil
}

func listChunksInTx(ctx context.Context, tx *sql.Tx, bucket, key string) ([]Chunk, error) {
	rows, err := tx.QueryContext(ctx, `
	SELECT bucket, key, part_number, offset, size, telegram_type, telegram_file_id, telegram_message_id, telegram_file_unique_id, sha256
	FROM object_chunks
	WHERE bucket = ? AND key = ?
	ORDER BY part_number ASC
	`, bucket, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []Chunk
	for rows.Next() {
		chunk, err := scanChunk(rows)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}

func scanChunk(scanner objectScanner) (Chunk, error) {
	var chunk Chunk
	if err := scanner.Scan(&chunk.Bucket, &chunk.Key, &chunk.PartNumber, &chunk.Offset, &chunk.Size, &chunk.TelegramType, &chunk.TelegramFileID, &chunk.TelegramMessageID, &chunk.TelegramFileUniqueID, &chunk.SHA256); err != nil {
		return Chunk{}, err
	}
	return chunk, nil
}

func objectExistsInTx(ctx context.Context, tx *sql.Tx, bucket, key string) (bool, error) {
	row := tx.QueryRowContext(ctx, `SELECT 1 FROM objects WHERE bucket = ? AND key = ?`, bucket, key)
	var value int
	if err := row.Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func putObjectInTx(ctx context.Context, tx *sql.Tx, object Object, chunks []Chunk) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE bucket = ? AND key = ?`, object.Bucket, object.Key); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
	INSERT INTO objects (bucket, key, size, content_type, etag, sha256, last_modified, chunk_count, telegram_type, upload_strategy)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(bucket, key) DO UPDATE SET
		size = excluded.size,
		content_type = excluded.content_type,
		etag = excluded.etag,
		sha256 = excluded.sha256,
		last_modified = excluded.last_modified,
		chunk_count = excluded.chunk_count,
		telegram_type = excluded.telegram_type,
		upload_strategy = excluded.upload_strategy
	`, object.Bucket, object.Key, object.Size, object.ContentType, object.ETag, object.SHA256, object.LastModified.Unix(), object.ChunkCount, object.TelegramType, object.UploadStrategy); err != nil {
		return err
	}
	for _, chunk := range chunks {
		if _, err := tx.ExecContext(ctx, `
		INSERT INTO object_chunks (bucket, key, part_number, offset, size, telegram_type, telegram_file_id, telegram_message_id, telegram_file_unique_id, sha256)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, chunk.Bucket, chunk.Key, chunk.PartNumber, chunk.Offset, chunk.Size, chunk.TelegramType, chunk.TelegramFileID, chunk.TelegramMessageID, chunk.TelegramFileUniqueID, chunk.SHA256); err != nil {
			return err
		}
	}
	return nil
}

func copyChunksForKey(chunks []Chunk, bucket, key string) []Chunk {
	copied := make([]Chunk, len(chunks))
	for i, chunk := range chunks {
		chunk.Bucket = bucket
		chunk.Key = key
		copied[i] = chunk
	}
	return copied
}

func deleteObjectInTx(ctx context.Context, tx *sql.Tx, bucket, key string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE bucket = ? AND key = ?`, bucket, key); err != nil {
		return err
	}
	return nil
}

func copyPrefixInTx(ctx context.Context, tx *sql.Tx, bucket, srcPrefix, dstPrefix string, overwrite bool) (bool, error) {
	rows, err := tx.QueryContext(ctx, `
	SELECT bucket, key, size, content_type, etag, sha256, last_modified, chunk_count, telegram_type, upload_strategy
	FROM objects
	WHERE bucket = ?
	  AND key LIKE ? ESCAPE '\'
	ORDER BY key ASC
	`, bucket, escapeSQLiteLikePattern(srcPrefix)+"%")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var objects []Object
	for rows.Next() {
		object, err := scanObject(rows)
		if err != nil {
			return false, err
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	created := true
	now := time.Now().UTC()
	for _, src := range objects {
		dstKey := dstPrefix + strings.TrimPrefix(src.Key, srcPrefix)
		dstExists, err := objectExistsInTx(ctx, tx, bucket, dstKey)
		if err != nil {
			return false, err
		}
		if dstExists {
			created = false
			if !overwrite {
				return false, errors.New("destination already exists")
			}
		}
		chunks, err := listChunksInTx(ctx, tx, bucket, src.Key)
		if err != nil {
			return false, err
		}
		if err := putObjectInTx(ctx, tx, Object{
			Bucket:         bucket,
			Key:            dstKey,
			Size:           src.Size,
			ContentType:    src.ContentType,
			ETag:           src.ETag,
			SHA256:         src.SHA256,
			LastModified:   now,
			ChunkCount:     src.ChunkCount,
			TelegramType:   src.TelegramType,
			UploadStrategy: src.UploadStrategy,
		}, copyChunksForKey(chunks, bucket, dstKey)); err != nil {
			return false, err
		}
	}
	return created, nil
}

func deletePrefixInTx(ctx context.Context, tx *sql.Tx, bucket, prefix string) error {
	likePattern := escapeSQLiteLikePattern(prefix) + "%"
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_chunks WHERE bucket = ? AND key LIKE ? ESCAPE '\'`, bucket, likePattern); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE bucket = ? AND key LIKE ? ESCAPE '\'`, bucket, likePattern); err != nil {
		return err
	}
	return nil
}

func scanObject(scanner objectScanner) (Object, error) {
	var object Object
	var lastModified int64
	if err := scanner.Scan(&object.Bucket, &object.Key, &object.Size, &object.ContentType, &object.ETag, &object.SHA256, &lastModified, &object.ChunkCount, &object.TelegramType, &object.UploadStrategy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Object{}, ErrNotFound
		}
		return Object{}, err
	}

	object.LastModified = unixSeconds(lastModified)
	return object, nil
}

func escapeSQLiteLikePattern(value string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`%`, `\%`,
		`_`, `\_`,
	)
	return replacer.Replace(value)
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func unixSeconds(value int64) time.Time {
	return time.Unix(value, 0)
}
