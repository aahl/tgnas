package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/aahl/tgnas/config"
	"github.com/aahl/tgnas/internal/s3api"
	"github.com/aahl/tgnas/metadata"
	"github.com/aahl/tgnas/store"
	"github.com/aahl/tgnas/telegram"
)

func TestRunReturnsObjectStoreCreationFailure(t *testing.T) {
	t.Setenv("TGNAS_TELEGRAM_BOT_TOKEN", "123456:valid-token")
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, configPath, filepath.Join(t.TempDir(), "metadata.sqlite"))

	storeErr := errors.New("startup bucket snapshot: metadata unavailable")
	oldNewObjectStore := newObjectStore
	oldListenAndServe := listenAndServe
	newObjectStore = func(metadata.Store, telegram.Client, store.Options) (*store.ObjectStore, error) {
		return nil, storeErr
	}
	listenAndServe = func(string, http.Handler) error {
		t.Fatal("listenAndServe should not be called")
		return nil
	}
	t.Cleanup(func() {
		newObjectStore = oldNewObjectStore
		listenAndServe = oldListenAndServe
	})

	err := run(configPath)
	if err == nil {
		t.Fatal("expected run error")
	}
	if !strings.Contains(err.Error(), "create object store") || !strings.Contains(err.Error(), "metadata unavailable") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseGlobalFlagsDefaultsToDataConfig(t *testing.T) {
	configPath, debug, rest, err := parseGlobalFlags(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseGlobalFlags returned error: %v", err)
	}
	if configPath != "data/config.yaml" {
		t.Fatalf("configPath = %q", configPath)
	}
	if debug {
		t.Fatal("debug = true, want false")
	}
	if len(rest) != 0 {
		t.Fatalf("rest = %v", rest)
	}
}

func TestParseGlobalFlagsAcceptsConfigAliases(t *testing.T) {
	configPath, debug, rest, err := parseGlobalFlags([]string{"-config", "config-a.yaml", "ls", "photos"}, io.Discard)
	if err != nil {
		t.Fatalf("parseGlobalFlags returned error: %v", err)
	}
	if configPath != "config-a.yaml" {
		t.Fatalf("configPath = %q", configPath)
	}
	if debug {
		t.Fatal("debug = true, want false")
	}
	if strings.Join(rest, " ") != "ls photos" {
		t.Fatalf("rest = %v", rest)
	}

	configPath, debug, rest, err = parseGlobalFlags([]string{"-c", "config-b.yaml", "lsd"}, io.Discard)
	if err != nil {
		t.Fatalf("parseGlobalFlags returned error: %v", err)
	}
	if configPath != "config-b.yaml" {
		t.Fatalf("configPath = %q", configPath)
	}
	if debug {
		t.Fatal("debug = true, want false")
	}
	if strings.Join(rest, " ") != "lsd" {
		t.Fatalf("rest = %v", rest)
	}
}

func TestParseGlobalFlagsAcceptsDebugFlag(t *testing.T) {
	configPath, debug, rest, err := parseGlobalFlags([]string{"-debug", "ls", "photos"}, io.Discard)
	if err != nil {
		t.Fatalf("parseGlobalFlags returned error: %v", err)
	}
	if configPath != "data/config.yaml" {
		t.Fatalf("configPath = %q", configPath)
	}
	if !debug {
		t.Fatal("debug = false, want true")
	}
	if strings.Join(rest, " ") != "ls photos" {
		t.Fatalf("rest = %v", rest)
	}
}

func TestRunMainStartsServerWithoutSubcommand(t *testing.T) {
	t.Setenv("TGNAS_TELEGRAM_BOT_TOKEN", "123456:valid-token")
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, configPath, filepath.Join(t.TempDir(), "metadata.sqlite"))

	oldRunServiceFunc := runServiceFunc
	runServiceFunc = func(configPath string, mode serverMode, dbg debugLogger) error {
		if mode != serverModeAll {
			t.Fatalf("mode = %q, want %q", mode, serverModeAll)
		}
		return errors.New("server stopped")
	}
	t.Cleanup(func() { runServiceFunc = oldRunServiceFunc })

	err := runMain([]string{"-c", configPath}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "server stopped") {
		t.Fatalf("err = %v, want server stopped", err)
	}
}

func TestRunMainS3Subcommand(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, configPath, filepath.Join(t.TempDir(), "metadata.sqlite"))

	called := false
	oldRunServiceFunc := runServiceFunc
	runServiceFunc = func(configPath string, mode serverMode, dbg debugLogger) error {
		called = true
		if mode != serverModeS3 {
			t.Fatalf("mode = %q, want %q", mode, serverModeS3)
		}
		return nil
	}
	t.Cleanup(func() { runServiceFunc = oldRunServiceFunc })

	if err := runMain([]string{"-c", configPath, "s3"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("runMain returned error: %v", err)
	}
	if !called {
		t.Fatal("runServiceFunc was not called")
	}
}

func TestRunMainDAVSubcommand(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, configPath, filepath.Join(t.TempDir(), "metadata.sqlite"))

	called := false
	oldRunServiceFunc := runServiceFunc
	runServiceFunc = func(configPath string, mode serverMode, dbg debugLogger) error {
		called = true
		if mode != serverModeDAV {
			t.Fatalf("mode = %q, want %q", mode, serverModeDAV)
		}
		return nil
	}
	t.Cleanup(func() { runServiceFunc = oldRunServiceFunc })

	if err := runMain([]string{"-c", configPath, "dav"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("runMain returned error: %v", err)
	}
	if !called {
		t.Fatal("runServiceFunc was not called")
	}
}

func TestPublicReadBucketsFromConfig(t *testing.T) {
	cfg := config.Config{Buckets: map[string]config.BucketConfig{
		"photos":  {ChatID: "-100", PublicRead: true},
		"backups": {ChatID: "-200"},
		"media":   {ChatID: "-300", PublicRead: true},
	}}

	publicReadBuckets := publicReadBucketsFromConfig(cfg)
	if !publicReadBuckets["photos"] || !publicReadBuckets["media"] {
		t.Fatalf("publicReadBuckets = %#v, want photos and media", publicReadBuckets)
	}
	if publicReadBuckets["backups"] {
		t.Fatalf("publicReadBuckets = %#v, backups should not be public", publicReadBuckets)
	}
}

func TestRunServiceWithDebugRoutesByMode(t *testing.T) {
	for _, tc := range []struct {
		name          string
		mode          serverMode
		davStatus     int
		bareDAVStatus int
		s3Status      int
		healthCode    int
	}{
		{name: "all", mode: serverModeAll, davStatus: http.StatusUnauthorized, bareDAVStatus: http.StatusPermanentRedirect, s3Status: http.StatusForbidden, healthCode: http.StatusOK},
		{name: "s3", mode: serverModeS3, davStatus: http.StatusForbidden, bareDAVStatus: http.StatusForbidden, s3Status: http.StatusForbidden, healthCode: http.StatusOK},
		{name: "dav", mode: serverModeDAV, davStatus: http.StatusUnauthorized, bareDAVStatus: http.StatusPermanentRedirect, s3Status: http.StatusNotFound, healthCode: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TGNAS_TELEGRAM_BOT_TOKEN", "123456:valid-token")
			t.Setenv("TGNAS_SECRET_KEY", "secret")
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			writeConfig(t, configPath, filepath.Join(t.TempDir(), "metadata.sqlite"))

			var handler http.Handler
			oldListenAndServe := listenAndServe
			listenAndServe = func(_ string, h http.Handler) error {
				handler = h
				return nil
			}
			t.Cleanup(func() { listenAndServe = oldListenAndServe })

			if err := runServiceWithDebug(configPath, tc.mode, newDebugLogger(false, io.Discard)); err != nil {
				t.Fatalf("runServiceWithDebug returned error: %v", err)
			}
			if handler == nil {
				t.Fatal("listenAndServe did not receive a handler")
			}

			davReq := httptest.NewRequest(http.MethodOptions, "/dav/", nil)
			davRec := httptest.NewRecorder()
			handler.ServeHTTP(davRec, davReq)
			if davRec.Code != tc.davStatus {
				t.Fatalf("dav status = %d, want %d", davRec.Code, tc.davStatus)
			}

			bareDAVReq := httptest.NewRequest(http.MethodGet, "/dav", nil)
			bareDAVRec := httptest.NewRecorder()
			handler.ServeHTTP(bareDAVRec, bareDAVReq)
			if bareDAVRec.Code != tc.bareDAVStatus {
				t.Fatalf("bare dav status = %d, want %d", bareDAVRec.Code, tc.bareDAVStatus)
			}
			if tc.bareDAVStatus == http.StatusPermanentRedirect && bareDAVRec.Header().Get("Location") != "/dav/" {
				t.Fatalf("bare dav Location = %q, want /dav/", bareDAVRec.Header().Get("Location"))
			}

			s3Req := httptest.NewRequest(http.MethodGet, "/photos", nil)
			s3Rec := httptest.NewRecorder()
			handler.ServeHTTP(s3Rec, s3Req)
			if s3Rec.Code != tc.s3Status {
				t.Fatalf("s3 status = %d, want %d", s3Rec.Code, tc.s3Status)
			}

			healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			healthRec := httptest.NewRecorder()
			handler.ServeHTTP(healthRec, healthReq)
			if healthRec.Code != tc.healthCode {
				t.Fatalf("health status = %d, want %d", healthRec.Code, tc.healthCode)
			}
		})
	}
}

func TestRunMainRejectsBothConfigAliases(t *testing.T) {
	err := runMain([]string{"-config", "a.yaml", "-c", "b.yaml"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "-config and -c cannot both be set") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunMainRejectsDebugFlagAfterSubcommand(t *testing.T) {
	err := runMain([]string{"ls", "-debug", "photos"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunMainDebugLogsServiceModeToStderr(t *testing.T) {
	t.Setenv("TGNAS_TELEGRAM_BOT_TOKEN", "123456:valid-token")
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	writeConfig(t, configPath, sqlitePath)

	oldListenAndServe := listenAndServe
	listenAndServe = func(string, http.Handler) error { return nil }
	t.Cleanup(func() { listenAndServe = oldListenAndServe })

	var stderr strings.Builder
	err := runMain([]string{"-debug", "-c", configPath}, io.Discard, &stderr)
	if err != nil {
		t.Fatalf("runMain returned error: %v", err)
	}
	for _, want := range []string{"debug mode=all", "config_path=" + strconv.Quote(configPath), "sqlite_path=" + strconv.Quote(sqlitePath), "listen_addr=" + strconv.Quote("127.0.0.1:0"), "bucket=" + strconv.Quote("photos")} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr = %q, want substring %q", stderr.String(), want)
		}
	}
}

func TestRunMainDebugLogsLSModeToStderrWithoutChangingStdout(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()
	seedBucket(t, meta, "photos", true)
	seedObject(t, meta, "photos", "2026/a.txt")
	seedObject(t, meta, "photos", "2026/b.txt")

	var stdout strings.Builder
	var stderr strings.Builder
	if err := runMain([]string{"-debug", "-c", configPath, "ls", "-n", "1", "photos/2026/"}, &stdout, &stderr); err != nil {
		t.Fatalf("runMain returned error: %v", err)
	}
	if got, want := stdout.String(), "2026/a.txt\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	for _, want := range []string{"debug mode=ls", "config_path=" + strconv.Quote(configPath), "sqlite_path=" + strconv.Quote(sqlitePath), "bucket=" + strconv.Quote("photos"), "prefix=" + strconv.Quote("2026/"), "limit=1", "page_after=", "page_limit=1", "rows=1"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr = %q, want substring %q", stderr.String(), want)
		}
	}
}

func TestRunMainDebugLogsQuoteUserControlledFields(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()
	seedBucket(t, meta, "photos", true)
	seedObject(t, meta, "photos", "2026/line\nbreak.txt")

	var stdout strings.Builder
	var stderr strings.Builder
	if err := runMain([]string{"-debug", "-c", configPath, "ls", "photos/2026/line\n"}, &stdout, &stderr); err != nil {
		t.Fatalf("runMain returned error: %v", err)
	}
	if got, want := stdout.String(), "2026/line\nbreak.txt\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	output := stderr.String()
	if !strings.Contains(output, `prefix="2026/line\n"`) || strings.Contains(output, "prefix=2026/line\n") {
		t.Fatalf("stderr did not quote prefix safely: %q", output)
	}
}

func TestRunMainDebugLogsLSDModeToStderr(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()
	seedBucket(t, meta, "photos", true)
	seedObject(t, meta, "photos", "2026/jan/a.txt")

	var stdout strings.Builder
	var stderr strings.Builder
	if err := runMain([]string{"-debug", "-c", configPath, "lsd", "photos/2026/"}, &stdout, &stderr); err != nil {
		t.Fatalf("runMain returned error: %v", err)
	}
	if got, want := stdout.String(), "2026/jan/\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	for _, want := range []string{"debug mode=lsd", "config_path=" + strconv.Quote(configPath), "sqlite_path=" + strconv.Quote(sqlitePath), "bucket=" + strconv.Quote("photos"), "prefix=" + strconv.Quote("2026/"), "page_after=", "page_limit=1000", "rows=1"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr = %q, want substring %q", stderr.String(), want)
		}
	}
}

func TestRunMainDebugLogsQuoteLSDPageCursor(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()
	seedBucket(t, meta, "photos", true)
	for i := 0; i < 999; i++ {
		seedObject(t, meta, "photos", fmt.Sprintf("2026/a%03d/file.txt", i))
	}
	cursorKey := "2026/a999\nbreak/file.txt"
	seedObject(t, meta, "photos", cursorKey)
	seedObject(t, meta, "photos", "2026/zoo/file.txt")

	var stdout strings.Builder
	var stderr strings.Builder
	if err := runMain([]string{"-debug", "-c", configPath, "lsd", "photos/2026/"}, &stdout, &stderr); err != nil {
		t.Fatalf("runMain returned error: %v", err)
	}
	output := stderr.String()
	if !strings.Contains(output, "page_after="+strconv.Quote(cursorKey)) || strings.Contains(output, "page_after="+cursorKey) {
		t.Fatalf("stderr did not quote lsd cursor safely: %q", output)
	}
}

func TestRunMainGlobalHelpSucceedsAndWritesUsage(t *testing.T) {
	var errOut strings.Builder
	err := runMain([]string{"-h"}, io.Discard, &errOut)
	if err != nil {
		t.Fatalf("runMain returned error: %v", err)
	}
	got := errOut.String()
	want := "Usage:\n  tgnas [-debug] [-c|-config config.yaml]\n  tgnas [-debug] [-c|-config config.yaml] s3\n  tgnas [-debug] [-c|-config config.yaml] dav\n  tgnas [-debug] [-c|-config config.yaml] ls [-n|-limit N] bucket[/prefix]\n  tgnas [-debug] [-c|-config config.yaml] lsd [bucket[/prefix]]\n  tgnas [-debug] [-c|-config config.yaml] bucket rename [--dry-run] old-bucket new-bucket\n"
	if got != want {
		t.Fatalf("help output = %q, want %q", got, want)
	}
}

func TestRunMainLSHelpSucceedsAndWritesUsage(t *testing.T) {
	var errOut strings.Builder
	err := runMain([]string{"ls", "-h"}, io.Discard, &errOut)
	if err != nil {
		t.Fatalf("runMain returned error: %v", err)
	}
	got := errOut.String()
	for _, want := range []string{"Usage of ls:", "-limit int", "-n int", "default 1000"} {
		if !strings.Contains(got, want) {
			t.Fatalf("help output = %q, want substring %q", got, want)
		}
	}
}

func TestRunMainLSDHelpSucceedsAndWritesUsage(t *testing.T) {
	var errOut strings.Builder
	err := runMain([]string{"lsd", "-h"}, io.Discard, &errOut)
	if err != nil {
		t.Fatalf("runMain returned error: %v", err)
	}
	if got := errOut.String(); !strings.Contains(got, "Usage of lsd:") {
		t.Fatalf("help output = %q", got)
	}
}

func TestRunMainLocalCommandsDoNotStartServerPath(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	seedBucket(t, meta, "photos", true)
	if err := meta.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	oldNewObjectStore := newObjectStore
	oldListenAndServe := listenAndServe
	newObjectStore = func(metadata.Store, telegram.Client, store.Options) (*store.ObjectStore, error) {
		t.Fatal("newObjectStore should not be called")
		return nil, nil
	}
	listenAndServe = func(string, http.Handler) error {
		t.Fatal("listenAndServe should not be called")
		return nil
	}
	defer func() {
		newObjectStore = oldNewObjectStore
		listenAndServe = oldListenAndServe
	}()

	if err := runMain([]string{"-c", configPath, "ls", "photos"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("runMain ls returned error: %v", err)
	}
	var out strings.Builder
	if err := runMain([]string{"-c", configPath, "lsd"}, &out, io.Discard); err != nil {
		t.Fatalf("runMain lsd returned error: %v", err)
	}
	if out.String() != "photos\n" {
		t.Fatalf("lsd output = %q", out.String())
	}
}

func TestParseBucketPrefix(t *testing.T) {
	testCases := []struct {
		name       string
		input      string
		wantBucket string
		wantPrefix string
		wantErr    bool
	}{
		{name: "bucket only", input: "photos", wantBucket: "photos", wantPrefix: ""},
		{name: "bucket prefix", input: "photos/2026/", wantBucket: "photos", wantPrefix: "2026/"},
		{name: "empty", input: "", wantErr: true},
		{name: "empty bucket", input: "/2026/", wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bucket, prefix, err := parseBucketPrefix(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseBucketPrefix returned error: %v", err)
			}
			if bucket != tc.wantBucket || prefix != tc.wantPrefix {
				t.Fatalf("bucket=%q prefix=%q, want bucket=%q prefix=%q", bucket, prefix, tc.wantBucket, tc.wantPrefix)
			}
		})
	}
}

func TestOpenMetadataFromConfig(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	writeConfig(t, configPath, sqlitePath)

	writableMeta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	if err := writableMeta.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	meta, sqlitePath, err := openMetadataFromConfig(configPath)
	if err != nil {
		t.Fatalf("openMetadataFromConfig returned error: %v", err)
	}
	defer meta.Close()
	if sqlitePath == "" {
		t.Fatal("sqlitePath = empty")
	}
}

func TestOpenMetadataFromConfigDoesNotCreateMissingDatabase(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "missing.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)

	err := runLS(configPath, []string{"photos"}, io.Discard, io.Discard, newDebugLogger(false, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "open sqlite metadata") {
		t.Fatalf("err = %v", err)
	}
	if _, statErr := os.Stat(sqlitePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("sqlite path was created or stat failed unexpectedly: %v", statErr)
	}
}

func TestRunLSReadsMetadataWhenServiceConfigIsIncomplete(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("metadata:\n  sqlite_path: '"+sqlitePath+"'\n"), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()
	seedBucket(t, meta, "photos", true)
	seedObject(t, meta, "photos", "2026/a.txt")

	var out strings.Builder
	if err := runLS(configPath, []string{"photos/2026/"}, &out, io.Discard, newDebugLogger(false, io.Discard)); err != nil {
		t.Fatalf("runLS returned error: %v", err)
	}
	if got, want := out.String(), "2026/a.txt\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestRunLSRejectsUnknownLocalMetadataConfigField(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configData := "metadata:\n  sqlite_pth: '" + sqlitePath + "'\n"
	if err := os.WriteFile(configPath, []byte(configData), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	err := runLS(configPath, []string{"photos"}, io.Discard, io.Discard, newDebugLogger(false, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "field sqlite_pth not found") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunLSRejectsInvalidArguments(t *testing.T) {
	configPath := writeConfigWithPath(t, filepath.Join(t.TempDir(), "metadata.sqlite"))

	testCases := []struct {
		name string
		args []string
	}{
		{name: "missing path", args: nil},
		{name: "too many paths", args: []string{"photos", "extra"}},
		{name: "empty bucket", args: []string{"/prefix"}},
		{name: "negative limit", args: []string{"-limit", "-1", "photos"}},
		{name: "negative short limit", args: []string{"-n", "-1", "photos"}},
		{name: "both limits", args: []string{"-limit", "1", "-n", "2", "photos"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := runLS(configPath, tc.args, io.Discard, io.Discard, newDebugLogger(false, io.Discard)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestRunLSListsObjectKeysWithPrefixAndLimit(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()

	seedBucket(t, meta, "photos", true)
	seedObject(t, meta, "photos", "2026/a.txt")
	seedObject(t, meta, "photos", "2026/b.txt")
	seedObject(t, meta, "photos", "2027/c.txt")

	var out strings.Builder
	if err := runLS(configPath, []string{"-n", "2", "photos/2026/"}, &out, io.Discard, newDebugLogger(false, io.Discard)); err != nil {
		t.Fatalf("runLS returned error: %v", err)
	}
	if got, want := out.String(), "2026/a.txt\n2026/b.txt\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestRunLSDefaultLimitIsOneThousand(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()

	seedBucket(t, meta, "photos", true)
	for i := 0; i < 1005; i++ {
		seedObject(t, meta, "photos", fmt.Sprintf("k%04d", i))
	}

	var out strings.Builder
	if err := runLS(configPath, []string{"photos"}, &out, io.Discard, newDebugLogger(false, io.Discard)); err != nil {
		t.Fatalf("runLS returned error: %v", err)
	}
	if got, want := len(strings.Split(strings.TrimSpace(out.String()), "\n")), 1000; got != want {
		t.Fatalf("line count = %d, want %d", got, want)
	}
}

func TestRunLSZeroLimitListsAllAcrossPages(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()

	seedBucket(t, meta, "photos", true)
	for i := 0; i < 1005; i++ {
		seedObject(t, meta, "photos", fmt.Sprintf("k%04d", i))
	}

	var out strings.Builder
	if err := runLS(configPath, []string{"-n", "0", "photos"}, &out, io.Discard, newDebugLogger(false, io.Discard)); err != nil {
		t.Fatalf("runLS returned error: %v", err)
	}
	if got, want := len(strings.Split(strings.TrimSpace(out.String()), "\n")), 1005; got != want {
		t.Fatalf("line count = %d, want %d", got, want)
	}
}

func TestRunLSRejectsMissingOrDisabledBucket(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()

	seedBucket(t, meta, "disabled", false)

	if err := runLS(configPath, []string{"missing"}, io.Discard, io.Discard, newDebugLogger(false, io.Discard)); err == nil || !strings.Contains(err.Error(), "bucket not found: missing") {
		t.Fatalf("err = %v", err)
	}
	if err := runLS(configPath, []string{"disabled"}, io.Discard, io.Discard, newDebugLogger(false, io.Discard)); err == nil || !strings.Contains(err.Error(), "bucket not found: disabled") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunLSDListsEnabledBuckets(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()
	seedBucket(t, meta, "backups", true)
	seedBucket(t, meta, "disabled", false)
	seedBucket(t, meta, "photos", true)

	var out strings.Builder
	if err := runLSD(configPath, nil, &out, io.Discard, newDebugLogger(false, io.Discard)); err != nil {
		t.Fatalf("runLSD returned error: %v", err)
	}
	if out.String() != "backups\nphotos\n" {
		t.Fatalf("output = %q", out.String())
	}
}

func TestRunLSDListsDirectCommonPrefixes(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()
	seedBucket(t, meta, "photos", true)
	seedObject(t, meta, "photos", "2026/a.txt")
	seedObject(t, meta, "photos", "2026/jan/a.txt")
	seedObject(t, meta, "photos", "2026/jan/b.txt")
	seedObject(t, meta, "photos", "2026/feb/c.txt")
	seedObject(t, meta, "photos", "2027/root.txt")

	var out strings.Builder
	if err := runLSD(configPath, []string{"photos/2026/"}, &out, io.Discard, newDebugLogger(false, io.Discard)); err != nil {
		t.Fatalf("runLSD returned error: %v", err)
	}
	if out.String() != "2026/feb/\n2026/jan/\n" {
		t.Fatalf("output = %q", out.String())
	}
}

func TestRunLSDRejectsInvalidArgumentsAndMissingBucket(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()

	seedBucket(t, meta, "disabled", false)

	if err := runLSD(configPath, []string{"photos", "extra"}, io.Discard, io.Discard, newDebugLogger(false, io.Discard)); err == nil {
		t.Fatal("expected too many args error")
	}
	if err := runLSD(configPath, []string{"-n", "1", "photos"}, io.Discard, io.Discard, newDebugLogger(false, io.Discard)); err == nil {
		t.Fatal("expected unsupported -n error")
	}
	if err := runLSD(configPath, []string{"-limit", "1", "photos"}, io.Discard, io.Discard, newDebugLogger(false, io.Discard)); err == nil {
		t.Fatal("expected unsupported -limit error")
	}
	if err := runLSD(configPath, []string{"/prefix"}, io.Discard, io.Discard, newDebugLogger(false, io.Discard)); err == nil {
		t.Fatal("expected empty bucket error")
	}
	if err := runLSD(configPath, []string{"missing"}, io.Discard, io.Discard, newDebugLogger(false, io.Discard)); err == nil || !strings.Contains(err.Error(), "bucket not found: missing") {
		t.Fatalf("err = %v", err)
	}
	if err := runLSD(configPath, []string{"disabled"}, io.Discard, io.Discard, newDebugLogger(false, io.Discard)); err == nil || !strings.Contains(err.Error(), "bucket not found: disabled") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunLSDDeduplicatesCommonPrefixesAcrossPageBoundary(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := writeConfigWithPath(t, sqlitePath)
	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()

	seedBucket(t, meta, "photos", true)
	for i := 0; i < 1001; i++ {
		seedObject(t, meta, "photos", fmt.Sprintf("2026/jan/k%04d.txt", i))
	}
	seedObject(t, meta, "photos", "2026/zoo/a.txt")

	var out strings.Builder
	if err := runLSD(configPath, []string{"photos/2026/"}, &out, io.Discard, newDebugLogger(false, io.Discard)); err != nil {
		t.Fatalf("runLSD returned error: %v", err)
	}
	if got, want := out.String(), "2026/jan/\n2026/zoo/\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestRunServiceWithDebugDisablesOrphanBuckets(t *testing.T) {
	t.Setenv("TGNAS_TELEGRAM_BOT_TOKEN", "123456:valid-token")
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	sqlitePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, configPath, sqlitePath)

	meta, err := metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	ctx := context.Background()
	if err := meta.UpsertBucket(ctx, metadata.Bucket{Name: "photos", ChatID: "-100", CreatedAt: time.Now().UTC(), Enabled: true}); err != nil {
		t.Fatalf("UpsertBucket photos returned error: %v", err)
	}
	if err := meta.UpsertBucket(ctx, metadata.Bucket{Name: "archive", ChatID: "-200", CreatedAt: time.Now().UTC(), Enabled: true}); err != nil {
		t.Fatalf("UpsertBucket archive returned error: %v", err)
	}
	if err := meta.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	oldListenAndServe := listenAndServe
	listenAndServe = func(string, http.Handler) error { return nil }
	t.Cleanup(func() { listenAndServe = oldListenAndServe })

	if err := runServiceWithDebug(configPath, serverModeAll, newDebugLogger(false, io.Discard)); err != nil {
		t.Fatalf("runServiceWithDebug returned error: %v", err)
	}

	meta, err = metadata.OpenSQLite(sqlitePath)
	if err != nil {
		t.Fatalf("OpenSQLite returned error: %v", err)
	}
	defer meta.Close()

	photos, err := meta.GetBucket(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucket photos returned error: %v", err)
	}
	if !photos.Enabled {
		t.Fatal("photos should remain enabled")
	}
	archive, err := meta.GetBucket(ctx, "archive")
	if err != nil {
		t.Fatalf("GetBucket archive returned error: %v", err)
	}
	if archive.Enabled {
		t.Fatal("archive should be disabled (orphan bucket)")
	}
}

func TestRunServiceCreatesBucketSpecificTelegramClients(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	metadataPath := filepath.Join(t.TempDir(), "metadata.sqlite")
	err := os.WriteFile(configPath, []byte(`
auth:
  credentials:
    - access_key: "admin"
      secret_key_env: "SECRET"
telegram:
  bot_token: "${TGNAS_GLOBAL_TOKEN}"
  api_base_url: "https://api.telegram.org"
metadata:
  sqlite_path: '`+metadataPath+`'
buckets:
  photos:
    chat_id: "-100123"
    bot_token: "${TGNAS_PHOTOS_TOKEN}"
  archive:
    chat_id: "-100456"
`), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SECRET", "secret")
	t.Setenv("TGNAS_GLOBAL_TOKEN", "222:global")
	t.Setenv("TGNAS_PHOTOS_TOKEN", "111:photos")

	captured := store.Options{}
	oldNewObjectStore := newObjectStore
	newObjectStore = func(meta metadata.Store, tg telegram.Client, options store.Options) (*store.ObjectStore, error) {
		captured = options
		return store.NewObjectStore(meta, tg, options)
	}
	defer func() { newObjectStore = oldNewObjectStore }()
	oldListen := listenAndServe
	listenAndServe = func(addr string, handler http.Handler) error { return errors.New("stop after setup") }
	defer func() { listenAndServe = oldListen }()

	err = runServiceWithDebug(configPath, serverModeS3, newDebugLogger(false, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "stop after setup") {
		t.Fatalf("runServiceWithDebug error = %v", err)
	}
	if len(captured.Buckets) != 2 {
		t.Fatalf("len(captured.Buckets) = %d, want 2", len(captured.Buckets))
	}
	if captured.Buckets["photos"].TokenSource != "bucket" {
		t.Fatalf("photos source = %q, want bucket", captured.Buckets["photos"].TokenSource)
	}
	if captured.Buckets["archive"].TokenSource != "global" {
		t.Fatalf("archive source = %q, want global", captured.Buckets["archive"].TokenSource)
	}
}

func TestModuleAndImportsUseTgnasPath(t *testing.T) {
	repoRoot := filepath.Join("..", "..")
	oldPath := "github.com/aahl/" + "tgs3"
	newPath := "github.com/aahl/tgnas"

	goMod, err := os.ReadFile(filepath.Join(repoRoot, "go.mod"))
	if err != nil {
		t.Fatalf("ReadFile go.mod returned error: %v", err)
	}
	if !strings.Contains(string(goMod), "module "+newPath) {
		t.Fatalf("go.mod module path does not contain %q", newPath)
	}
	if strings.Contains(string(goMod), oldPath) {
		t.Fatalf("go.mod still contains old path %q", oldPath)
	}

	if err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".claude", "tgnas":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), oldPath) {
			t.Fatalf("%s still contains old path %q", path, oldPath)
		}
		return nil
	}); err != nil {
		t.Fatalf("WalkDir returned error: %v", err)
	}
}

func writeConfig(t *testing.T, path, sqlitePath string) {
	t.Helper()
	config := `server:
  listen: "127.0.0.1:0"
auth:
  region: "us-east-1"
  credentials:
    - access_key: "admin"
      secret_key_env: "TGNAS_SECRET_KEY"
telegram:
  bot_token_env: "TGNAS_TELEGRAM_BOT_TOKEN"
metadata:
  sqlite_path: '` + sqlitePath + `'
buckets:
  photos:
    chat_id: "-100"
`
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
}

func writeConfigWithPath(t *testing.T, sqlitePath string) string {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	writeConfig(t, configPath, sqlitePath)
	return configPath
}

func seedBucket(t *testing.T, meta metadata.Store, name string, enabled bool) {
	t.Helper()
	if err := meta.UpsertBucket(context.Background(), metadata.Bucket{Name: name, ChatID: "-100", CreatedAt: time.Now().UTC(), Enabled: enabled}); err != nil {
		t.Fatalf("UpsertBucket returned error: %v", err)
	}
}

func seedObject(t *testing.T, meta metadata.Store, bucket, key string) {
	t.Helper()
	object := metadata.Object{Bucket: bucket, Key: key, Size: int64(len(key)), ContentType: "text/plain", ETag: key + "-etag", SHA256: key + "-sha", LastModified: time.Now().UTC(), ChunkCount: 1, TelegramType: "document", UploadStrategy: "document"}
	chunk := metadata.Chunk{Bucket: bucket, Key: key, PartNumber: 1, Offset: 0, Size: object.Size, TelegramType: "document", TelegramFileID: key + "-file", TelegramMessageID: 1, TelegramFileUniqueID: key + "-unique", SHA256: object.SHA256}
	if err := meta.PutObject(context.Background(), object, []metadata.Chunk{chunk}); err != nil {
		t.Fatalf("PutObject(%s) returned error: %v", key, err)
	}
}

func TestRunMainBucketRenameDryRunDoesNotModifyMetadata(t *testing.T) {
	t.Setenv("TGNAS_TELEGRAM_BOT_TOKEN", "123456:valid-token")
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	dbPath := filepath.Join(dir, "metadata.sqlite")

	cfg := `server:
  listen: "127.0.0.1:0"
auth:
  region: "us-east-1"
  credentials:
    - access_key: "admin"
      secret_key_env: "TGNAS_SECRET_KEY"
telegram:
  bot_token_env: "TGNAS_TELEGRAM_BOT_TOKEN"
metadata:
  sqlite_path: '` + dbPath + `'
buckets:
  new:
    chat_id: "-100555"
`
	if err := os.WriteFile(configPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	meta, err := metadata.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	if err := meta.UpsertBucket(context.Background(), metadata.Bucket{Name: "old", ChatID: "-100555", CreatedAt: createdAt, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := meta.PutObject(context.Background(), metadata.Object{Bucket: "old", Key: "file.txt", Size: 4, ContentType: "text/plain", ETag: "etag3", SHA256: "sha3", LastModified: createdAt, ChunkCount: 1, TelegramType: "document", UploadStrategy: "single"}, []metadata.Chunk{{Bucket: "old", Key: "file.txt", PartNumber: 1, Offset: 0, Size: 4, TelegramType: "document", TelegramFileID: "f3", SHA256: "csha3"}}); err != nil {
		t.Fatal(err)
	}
	meta.Close()

	var stdout, stderr bytes.Buffer
	if err := runMain([]string{"-c", configPath, "bucket", "rename", "--dry-run", "old", "new"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	out := stdout.String()
	if !strings.Contains(out, "would rename bucket old to new: buckets=1") {
		t.Fatalf("unexpected stdout: %s", out)
	}

	meta, err = metadata.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()
	_, err = meta.GetBucket(context.Background(), "old")
	if err != nil {
		t.Fatalf("dry-run should not have removed old bucket: %v", err)
	}
}

func TestRunMainBucketRenameRenamesMetadata(t *testing.T) {
	t.Setenv("TGNAS_TELEGRAM_BOT_TOKEN", "123456:valid-token")
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	dbPath := filepath.Join(dir, "metadata.sqlite")

	cfg := `server:
  listen: "127.0.0.1:0"
auth:
  region: "us-east-1"
  credentials:
    - access_key: "admin"
      secret_key_env: "TGNAS_SECRET_KEY"
telegram:
  bot_token_env: "TGNAS_TELEGRAM_BOT_TOKEN"
metadata:
  sqlite_path: '` + dbPath + `'
buckets:
  new:
    chat_id: "-100666"
`
	if err := os.WriteFile(configPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	meta, err := metadata.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	if err := meta.UpsertBucket(context.Background(), metadata.Bucket{Name: "old", ChatID: "-100666", CreatedAt: createdAt, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	meta.Close()

	var stdout, stderr bytes.Buffer
	if err := runMain([]string{"-c", configPath, "bucket", "rename", "old", "new"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	out := stdout.String()
	if !strings.Contains(out, "renamed bucket old to new: buckets=1") {
		t.Fatalf("unexpected stdout: %s", out)
	}

	meta, err = metadata.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()
	_, err = meta.GetBucket(context.Background(), "old")
	if err != metadata.ErrNotFound {
		t.Fatalf("expected old bucket gone after rename: %v", err)
	}
	bucket, err := meta.GetBucket(context.Background(), "new")
	if err != nil {
		t.Fatal(err)
	}
	if bucket.ChatID != "-100666" {
		t.Fatalf("chat_id not preserved: %s", bucket.ChatID)
	}
}

func TestRunMainBucketRenameRequiresConfiguredTarget(t *testing.T) {
	t.Setenv("TGNAS_TELEGRAM_BOT_TOKEN", "123456:valid-token")
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	dbPath := filepath.Join(dir, "metadata.sqlite")

	cfg := `server:
  listen: "127.0.0.1:0"
auth:
  region: "us-east-1"
  credentials:
    - access_key: "admin"
      secret_key_env: "TGNAS_SECRET_KEY"
telegram:
  bot_token_env: "TGNAS_TELEGRAM_BOT_TOKEN"
metadata:
  sqlite_path: '` + dbPath + `'
buckets:
  old:
    chat_id: "-100777"
`
	if err := os.WriteFile(configPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	meta, err := metadata.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	if err := meta.UpsertBucket(context.Background(), metadata.Bucket{Name: "old", ChatID: "-100777", CreatedAt: createdAt, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	meta.Close()

	var stdout, stderr bytes.Buffer
	err = runMain([]string{"-c", configPath, "bucket", "rename", "old", "new"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error when target bucket not configured")
	}
	if !strings.Contains(err.Error(), "target bucket is not configured") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunMainBucketRenameRejectsDifferentTargetChatID(t *testing.T) {
	t.Setenv("TGNAS_TELEGRAM_BOT_TOKEN", "123456:valid-token")
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	dbPath := filepath.Join(dir, "metadata.sqlite")

	cfg := `server:
  listen: "127.0.0.1:0"
auth:
  region: "us-east-1"
  credentials:
    - access_key: "admin"
      secret_key_env: "TGNAS_SECRET_KEY"
telegram:
  bot_token_env: "TGNAS_TELEGRAM_BOT_TOKEN"
metadata:
  sqlite_path: '` + dbPath + `'
buckets:
  new:
    chat_id: "-100999"
`
	if err := os.WriteFile(configPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	meta, err := metadata.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	if err := meta.UpsertBucket(context.Background(), metadata.Bucket{Name: "old", ChatID: "-100888", CreatedAt: createdAt, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	meta.Close()

	var stdout, stderr bytes.Buffer
	err = runMain([]string{"-c", configPath, "bucket", "rename", "old", "new"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error when target chat_id differs from source metadata")
	}
	if !strings.Contains(err.Error(), "target bucket chat_id differs") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunMainBucketRenameRejectsExistingTarget(t *testing.T) {
	t.Setenv("TGNAS_TELEGRAM_BOT_TOKEN", "123456:valid-token")
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	dbPath := filepath.Join(dir, "metadata.sqlite")

	cfg := `server:
  listen: "127.0.0.1:0"
auth:
  region: "us-east-1"
  credentials:
    - access_key: "admin"
      secret_key_env: "TGNAS_SECRET_KEY"
telegram:
  bot_token_env: "TGNAS_TELEGRAM_BOT_TOKEN"
metadata:
  sqlite_path: '` + dbPath + `'
buckets:
  new:
    chat_id: "-100111"
`
	if err := os.WriteFile(configPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	meta, err := metadata.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	_ = meta.UpsertBucket(context.Background(), metadata.Bucket{Name: "old", ChatID: "-100111", CreatedAt: createdAt, Enabled: true})
	_ = meta.UpsertBucket(context.Background(), metadata.Bucket{Name: "new", ChatID: "-100222", CreatedAt: createdAt, Enabled: true})
	meta.Close()

	var stdout, stderr bytes.Buffer
	err = runMain([]string{"-c", configPath, "bucket", "rename", "old", "new"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error when target bucket already exists in metadata")
	}
	if !strings.Contains(err.Error(), "destination bucket already exists") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunMainBucketRenameWarnsWhenSourceStillConfigured(t *testing.T) {
	t.Setenv("TGNAS_TELEGRAM_BOT_TOKEN", "123456:valid-token")
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	dbPath := filepath.Join(dir, "metadata.sqlite")

	cfg := `server:
  listen: "127.0.0.1:0"
auth:
  region: "us-east-1"
  credentials:
    - access_key: "admin"
      secret_key_env: "TGNAS_SECRET_KEY"
telegram:
  bot_token_env: "TGNAS_TELEGRAM_BOT_TOKEN"
metadata:
  sqlite_path: '` + dbPath + `'
buckets:
  old:
    chat_id: "-100333"
  new:
    chat_id: "-100333"
`
	if err := os.WriteFile(configPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	meta, err := metadata.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	if err := meta.UpsertBucket(context.Background(), metadata.Bucket{Name: "old", ChatID: "-100333", CreatedAt: createdAt, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	meta.Close()

	var stdout, stderr bytes.Buffer
	if err := runMain([]string{"-c", configPath, "bucket", "rename", "old", "new"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(stderr.String(), "warning: source bucket still exists in config") {
		t.Fatalf("expected stderr warning, got: %s", stderr.String())
	}
	if !strings.Contains(stdout.String(), "renamed bucket old to new") {
		t.Fatalf("expected success output, got: %s", stdout.String())
	}
}

func TestTrustedProxyMiddlewareLogsTrustDecision(t *testing.T) {
	var logs strings.Builder
	handler, err := newTrustedProxyMiddlewareWithLogger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), config.ServerConfig{TrustedProxies: []string{"127.0.0.1/32"}}, log.New(&logs, "", 0))
	if err != nil {
		t.Fatalf("newTrustedProxyMiddlewareWithLogger returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9000/", nil)
	request.RemoteAddr = "127.0.0.1:54321"
	request.Header.Set("X-Forwarded-Host", "s3.example.com")
	request.Header.Set("X-Forwarded-Proto", "https")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}

	got := logs.String()
	for _, want := range []string{
		`event=trusted_proxy`,
		`remote_addr="127.0.0.1:54321"`,
		`original_host="127.0.0.1:9000"`,
		`forwarded_host="s3.example.com"`,
		`forwarded_proto="https"`,
		`trusted=true`,
		`rewritten_host="s3.example.com"`,
		`rewritten_scheme="https"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("log %q does not contain %s", got, want)
		}
	}
}

func TestTrustedProxyMiddlewareTrustsForwardedHostWhenRemoteIPMatches(t *testing.T) {
	handler, err := newTrustedProxyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "external.example.com" {
			t.Fatalf("Host = %q", r.Host)
		}
		if r.URL.Host != "external.example.com" {
			t.Fatalf("URL.Host = %q", r.URL.Host)
		}
		if r.URL.Scheme != "https" {
			t.Fatalf("URL.Scheme = %q", r.URL.Scheme)
		}
		w.WriteHeader(http.StatusNoContent)
	}), config.ServerConfig{TrustedProxies: []string{"127.0.0.1/32"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9000/", nil)
	request.RemoteAddr = "127.0.0.1:54321"
	request.Header.Set("X-Forwarded-Host", "external.example.com")
	request.Header.Set("X-Forwarded-Proto", "https")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTrustedProxyMiddlewareTrustsForwardedProtoWhenRemoteIPMatches(t *testing.T) {
	handler, err := newTrustedProxyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "127.0.0.1:9000" {
			t.Fatalf("Host = %q", r.Host)
		}
		if r.URL.Host != "127.0.0.1:9000" {
			t.Fatalf("URL.Host = %q", r.URL.Host)
		}
		if r.URL.Scheme != "https" {
			t.Fatalf("URL.Scheme = %q", r.URL.Scheme)
		}
		w.WriteHeader(http.StatusNoContent)
	}), config.ServerConfig{TrustedProxies: []string{"127.0.0.1/32"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9000/", nil)
	request.RemoteAddr = "127.0.0.1:54321"
	request.Header.Set("X-Forwarded-Proto", "https")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTrustedProxyMiddlewareClonesBeforeRewritingRequest(t *testing.T) {
	handler, err := newTrustedProxyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "external.example.com" {
			t.Fatalf("Host = %q", r.Host)
		}
		if r.URL.Host != "external.example.com" {
			t.Fatalf("URL.Host = %q", r.URL.Host)
		}
		if r.URL.Scheme != "https" {
			t.Fatalf("URL.Scheme = %q", r.URL.Scheme)
		}
		w.WriteHeader(http.StatusNoContent)
	}), config.ServerConfig{TrustedProxies: []string{"127.0.0.1/32"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://internal.example:9000/", nil)
	request.Host = "internal.example:9000"
	request.URL.Host = "internal.example:9000"
	request.URL.Scheme = "http"
	request.RemoteAddr = "127.0.0.1:54321"
	request.Header.Set("X-Forwarded-Host", "external.example.com")
	request.Header.Set("X-Forwarded-Proto", "https")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
	if request.Host != "internal.example:9000" {
		t.Fatalf("original Host = %q", request.Host)
	}
	if request.URL.Host != "internal.example:9000" {
		t.Fatalf("original URL.Host = %q", request.URL.Host)
	}
	if request.URL.Scheme != "http" {
		t.Fatalf("original URL.Scheme = %q", request.URL.Scheme)
	}
}

func TestTrustedProxyMiddlewareAcceptsAnyForwardedHostWhenRemoteIPMatches(t *testing.T) {
	handler, err := newTrustedProxyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "tenant.example.net" {
			t.Fatalf("Host = %q", r.Host)
		}
		w.WriteHeader(http.StatusNoContent)
	}), config.ServerConfig{TrustedProxies: []string{"10.0.0.0/8"}, TrustedProxyHosts: []string{"s3.example.com"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://10.1.2.3:9000/", nil)
	request.RemoteAddr = "10.1.2.3:54321"
	request.Header.Set("X-Forwarded-Host", "tenant.example.net")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTrustedProxyMiddlewareTrustsForwardedHostWhenHostMatches(t *testing.T) {
	handler, err := newTrustedProxyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "s3.example.com" {
			t.Fatalf("Host = %q", r.Host)
		}
		w.WriteHeader(http.StatusNoContent)
	}), config.ServerConfig{TrustedProxyHosts: []string{"s3.example.com"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9000/", nil)
	request.RemoteAddr = "203.0.113.10:54321"
	request.Header.Set("X-Forwarded-Host", "S3.EXAMPLE.COM")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTrustedProxyMiddlewareLeavesRequestUnchangedWithoutTrustMatch(t *testing.T) {
	handler, err := newTrustedProxyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "127.0.0.1:9000" {
			t.Fatalf("Host = %q", r.Host)
		}
		if r.URL.Host != "127.0.0.1:9000" {
			t.Fatalf("URL.Host = %q", r.URL.Host)
		}
		if r.URL.Scheme != "http" {
			t.Fatalf("URL.Scheme = %q", r.URL.Scheme)
		}
		w.WriteHeader(http.StatusNoContent)
	}), config.ServerConfig{TrustedProxies: []string{"10.0.0.0/8"}, TrustedProxyHosts: []string{"s3.example.com"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9000/", nil)
	request.RemoteAddr = "203.0.113.10:54321"
	request.Header.Set("X-Forwarded-Host", "evil.example.com")
	request.Header.Set("X-Forwarded-Proto", "https")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTrustedProxyMiddlewareReturnsOriginalHandlerWithoutTrustedSettings(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "127.0.0.1:9000" {
			t.Fatalf("Host = %q", r.Host)
		}
		if r.URL.Scheme != "http" {
			t.Fatalf("URL.Scheme = %q", r.URL.Scheme)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	handler, err := newTrustedProxyMiddleware(inner, config.ServerConfig{})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}
	if fmt.Sprintf("%p", handler) != fmt.Sprintf("%p", inner) {
		t.Fatalf("handler = %p, want original %p", handler, inner)
	}

	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9000/", nil)
	request.RemoteAddr = "127.0.0.1:54321"
	request.Header.Set("X-Forwarded-Host", "s3.example.com")
	request.Header.Set("X-Forwarded-Proto", "https")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTrustedProxyMiddlewareTrustsIPv6RemoteAddr(t *testing.T) {
	handler, err := newTrustedProxyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "ipv6.example.com" {
			t.Fatalf("Host = %q", r.Host)
		}
		w.WriteHeader(http.StatusNoContent)
	}), config.ServerConfig{TrustedProxies: []string{"2001:db8::/32"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://[2001:db8::1]:9000/", nil)
	request.RemoteAddr = "[2001:db8::1]:54321"
	request.Header.Set("X-Forwarded-Host", "ipv6.example.com")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTrustedProxyMiddlewarePrefersXForwardedHeaders(t *testing.T) {
	handler, err := newTrustedProxyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "x-forwarded.example.com" {
			t.Fatalf("Host = %q", r.Host)
		}
		if r.URL.Scheme != "https" {
			t.Fatalf("URL.Scheme = %q", r.URL.Scheme)
		}
		w.WriteHeader(http.StatusNoContent)
	}), config.ServerConfig{TrustedProxies: []string{"127.0.0.1/32"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9000/", nil)
	request.RemoteAddr = "127.0.0.1:54321"
	request.Header.Set("X-Forwarded-Host", "x-forwarded.example.com")
	request.Header.Set("X-Forwarded-Proto", "https")
	request.Header.Set("Forwarded", `proto=http;host="forwarded.example.com"`)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTrustedProxyMiddlewareReadsForwardedHeaderFallback(t *testing.T) {
	handler, err := newTrustedProxyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "s3.example.com" {
			t.Fatalf("Host = %q", r.Host)
		}
		if r.URL.Scheme != "https" {
			t.Fatalf("URL.Scheme = %q", r.URL.Scheme)
		}
		w.WriteHeader(http.StatusNoContent)
	}), config.ServerConfig{TrustedProxyHosts: []string{"s3.example.com"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9000/", nil)
	request.RemoteAddr = "203.0.113.10:54321"
	request.Header.Set("Forwarded", `for=203.0.113.9;proto=https;host="s3.example.com"`)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTrustedProxyMiddlewareUsesFirstForwardedHostValue(t *testing.T) {
	handler, err := newTrustedProxyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "first.example.com" {
			t.Fatalf("Host = %q", r.Host)
		}
		w.WriteHeader(http.StatusNoContent)
	}), config.ServerConfig{TrustedProxies: []string{"127.0.0.1/32"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9000/", nil)
	request.RemoteAddr = "127.0.0.1:54321"
	request.Header.Set("X-Forwarded-Host", "first.example.com, second.example.com")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTrustedProxyMiddlewareAllowsSigV4SignedForForwardedHost(t *testing.T) {
	s3Handler := s3api.NewServer(proxySigV4ObjectStore{}, s3api.Options{
		Region:      "us-east-1",
		Credentials: map[string]string{"AKID": "SECRET"},
		SigV4Clock:  func() time.Time { return time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC) },
		Ready:       func() bool { return true },
	})
	handler, err := newTrustedProxyMiddleware(s3Handler, config.ServerConfig{TrustedProxies: []string{"127.0.0.1/32"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "https://s3.example.com/", nil)
	signRequestForProxyTest(t, request)
	request.URL.Scheme = "http"
	request.URL.Host = "127.0.0.1:9000"
	request.Host = "127.0.0.1:9000"
	request.RemoteAddr = "127.0.0.1:54321"
	request.Header.Set("X-Forwarded-Host", "s3.example.com")
	request.Header.Set("X-Forwarded-Proto", "https")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestTrustedProxyMiddlewareAllowsPresignedURLSignedForForwardedHost(t *testing.T) {
	s3Handler := s3api.NewServer(proxySigV4ObjectStore{}, s3api.Options{
		Region:      "us-east-1",
		Credentials: map[string]string{"AKID": "SECRET"},
		SigV4Clock:  func() time.Time { return time.Date(2024, 1, 2, 3, 5, 0, 0, time.UTC) },
		Ready:       func() bool { return true },
	})
	handler, err := newTrustedProxyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "s3.example.com" {
			t.Fatalf("Host = %q", r.Host)
		}
		if r.URL.Host != "s3.example.com" {
			t.Fatalf("URL.Host = %q", r.URL.Host)
		}
		if r.URL.Scheme != "https" {
			t.Fatalf("URL.Scheme = %q", r.URL.Scheme)
		}
		s3Handler.ServeHTTP(w, r)
	}), config.ServerConfig{TrustedProxies: []string{"127.0.0.1/32"}})
	if err != nil {
		t.Fatalf("newTrustedProxyMiddleware returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodHead, "https://s3.example.com/tgnas/test.txt", nil)
	request.Host = "s3.example.com"
	query := request.URL.Query()
	query.Set("X-Amz-Expires", strconv.FormatInt(int64((15*time.Minute)/time.Second), 10))
	request.URL.RawQuery = query.Encode()

	credentials := aws.Credentials{AccessKeyID: "AKID", SecretAccessKey: "SECRET"}
	signedURL, _, err := v4.NewSigner().PresignHTTP(context.Background(), credentials, request, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), func(options *v4.SignerOptions) {
		options.DisableURIPathEscaping = true
	})
	if err != nil {
		t.Fatalf("PresignHTTP returned error: %v", err)
	}

	presigned := httptest.NewRequest(http.MethodHead, signedURL, nil)
	presigned.URL.Scheme = "http"
	presigned.URL.Host = "127.0.0.1:9000"
	presigned.Host = "127.0.0.1:9000"
	presigned.RemoteAddr = "127.0.0.1:54321"
	presigned.Header.Set("X-Forwarded-Host", "s3.example.com")
	presigned.Header.Set("X-Forwarded-Proto", "https")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, presigned)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

type proxySigV4ObjectStore struct{}

func (proxySigV4ObjectStore) ListBuckets(context.Context) ([]metadata.Bucket, error) {
	return []metadata.Bucket{{Name: "tgnas", CreatedAt: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), Enabled: true}}, nil
}

func (proxySigV4ObjectStore) HeadBucket(context.Context, string) error {
	return nil
}

func (proxySigV4ObjectStore) DeleteBucket(context.Context, string) error {
	return nil
}

func (proxySigV4ObjectStore) PutObject(context.Context, store.PutObjectInput) (store.PutObjectResult, error) {
	return store.PutObjectResult{}, nil
}

func (proxySigV4ObjectStore) GetObject(context.Context, store.GetObjectInput) (io.ReadCloser, store.ObjectInfo, error) {
	return nil, store.ObjectInfo{}, store.ErrNoSuchKey
}

func (proxySigV4ObjectStore) HeadObject(context.Context, string, string) (store.ObjectInfo, error) {
	return store.ObjectInfo{}, store.ErrNoSuchKey
}

func (proxySigV4ObjectStore) ListObjects(context.Context, store.ListObjectsInput) (store.ListObjectsResult, error) {
	return store.ListObjectsResult{}, nil
}

func (proxySigV4ObjectStore) DeleteObject(context.Context, string, string) error {
	return nil
}

func (proxySigV4ObjectStore) CreateMultipartUpload(context.Context, store.CreateMultipartUploadInput) (store.CreateMultipartUploadResult, error) {
	return store.CreateMultipartUploadResult{}, store.ErrNotImplemented
}

func (proxySigV4ObjectStore) UploadPart(context.Context, store.UploadPartInput) (store.UploadPartResult, error) {
	return store.UploadPartResult{}, store.ErrNotImplemented
}

func (proxySigV4ObjectStore) CompleteMultipartUpload(context.Context, store.CompleteMultipartUploadInput) (store.CompleteMultipartUploadResult, error) {
	return store.CompleteMultipartUploadResult{}, store.ErrNotImplemented
}

func (proxySigV4ObjectStore) AbortMultipartUpload(context.Context, store.AbortMultipartUploadInput) error {
	return store.ErrNotImplemented
}

func signRequestForProxyTest(t *testing.T, request *http.Request) {
	t.Helper()
	payloadHash := request.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		sum := sha256.Sum256(nil)
		payloadHash = hex.EncodeToString(sum[:])
		request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	}
	credentials := aws.Credentials{AccessKeyID: "AKID", SecretAccessKey: "SECRET"}
	if err := v4.NewSigner().SignHTTP(context.Background(), credentials, request, payloadHash, "s3", "us-east-1", time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)); err != nil {
		t.Fatalf("SignHTTP returned error: %v", err)
	}
}

func TestS3CORSProductionWiring(t *testing.T) {
	cfg := config.Config{
		Server: config.ServerConfig{AllowedOrigins: []string{"https://global.example"}},
		Auth:   config.AuthConfig{Region: "us-east-1"},
		Buckets: map[string]config.BucketConfig{
			"override":        {AllowedOrigins: []string{"https://bucket.example"}},
			"fallback-empty":  {AllowedOrigins: []string{}},
			"fallback-blanks": {AllowedOrigins: []string{"", ""}},
		},
	}
	policy, err := compileCORSPolicyFromConfig(cfg)
	if err != nil {
		t.Fatalf("compileCORSPolicyFromConfig returned error: %v", err)
	}
	options := s3OptionsFromConfig(cfg, map[string]string{"AKID": "SECRET"}, func() bool { return true }, log.New(io.Discard, "", 0), policy)
	server := s3api.NewServer(proxySigV4ObjectStore{}, options)

	tests := []struct {
		path   string
		origin string
		want   int
	}{
		{path: "/override/key", origin: "https://bucket.example", want: http.StatusNoContent},
		{path: "/override/key", origin: "https://global.example", want: http.StatusForbidden},
		{path: "/fallback-empty/key", origin: "https://global.example", want: http.StatusNoContent},
		{path: "/fallback-blanks/key", origin: "https://global.example", want: http.StatusNoContent},
		{path: "/", origin: "https://global.example", want: http.StatusNoContent},
	}
	for _, tc := range tests {
		req := httptest.NewRequest(http.MethodOptions, tc.path, nil)
		req.Header.Set("Origin", tc.origin)
		req.Header.Set("Access-Control-Request-Method", "GET")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("%s from %s: status = %d, want %d", tc.path, tc.origin, rec.Code, tc.want)
		}
	}
}

func TestRunServiceRejectsInvalidCORSBeforeSideEffects(t *testing.T) {
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	tests := []struct {
		name       string
		globalCORS string
		bucketCORS string
		wantText   string
	}{
		{name: "global", globalCORS: "  allowed_origins:\n    - /[invalid/\n", wantText: "global CORS policy"},
		{name: "bucket", bucketCORS: "    allowed_origins:\n      - /[invalid/\n", wantText: `bucket "photos" CORS policy`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			sqlitePath := filepath.Join(dir, "metadata.sqlite")
			configPath := filepath.Join(dir, "config.yaml")
			contents := fmt.Sprintf(`
server:
  listen: :0
%s
auth:
  credentials:
    - access_key: AKID
      secret_key_env: TGNAS_SECRET_KEY
telegram:
  bot_token: 123456:valid-token
metadata:
  sqlite_path: '%s'
buckets:
  photos:
    chat_id: "-100"
%s`, tc.globalCORS, sqlitePath, tc.bucketCORS)
			if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}

			objectStoreCalled := false
			oldNewObjectStore := newObjectStore
			oldListenAndServe := listenAndServe
			newObjectStore = func(metadata.Store, telegram.Client, store.Options) (*store.ObjectStore, error) {
				objectStoreCalled = true
				return nil, errors.New("unexpected object store construction")
			}
			listenAndServe = func(string, http.Handler) error {
				t.Fatal("listenAndServe should not be called")
				return nil
			}
			t.Cleanup(func() {
				newObjectStore = oldNewObjectStore
				listenAndServe = oldListenAndServe
			})

			err := runServiceWithDebug(configPath, serverModeS3, newDebugLogger(false, io.Discard))
			if err == nil || !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("err = %v, want text %q", err, tc.wantText)
			}
			if objectStoreCalled {
				t.Fatal("newObjectStore was called")
			}
			if _, statErr := os.Stat(sqlitePath); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("sqlite path stat error = %v, want not exist", statErr)
			}
		})
	}
}

func TestCombinedHandlerDoesNotApplyS3CORSToWebDAV(t *testing.T) {
	t.Setenv("TGNAS_SECRET_KEY", "secret")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	sqlitePath := filepath.Join(dir, "metadata.sqlite")
	contents := strings.Join([]string{
		"server:",
		"  listen: :0",
		"  allowed_origins:",
		"    - https://frontend.example",
		"auth:",
		"  credentials:",
		"    - access_key: AKID",
		"      secret_key_env: TGNAS_SECRET_KEY",
		"telegram:",
		"  bot_token: 123456:valid-token",
		"metadata:",
		"  sqlite_path: '" + sqlitePath + "'",
		"buckets:",
		"  photos:",
		"    chat_id: \"-100\"",
		"webdav:",
		"  prefix: /dav/",
		"",
	}, "\n")
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	var handler http.Handler
	oldListenAndServe := listenAndServe
	listenAndServe = func(_ string, h http.Handler) error {
		handler = h
		return nil
	}
	t.Cleanup(func() { listenAndServe = oldListenAndServe })
	if err := runServiceWithDebug(configPath, serverModeAll, newDebugLogger(false, io.Discard)); err != nil {
		t.Fatalf("runServiceWithDebug returned error: %v", err)
	}

	req := httptest.NewRequest(http.MethodOptions, "/dav/", nil)
	req.Header.Set("Origin", "https://frontend.example")
	req.Header.Set("Access-Control-Request-Method", "GET")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("WebDAV OPTIONS status = %d, want existing authentication status %d", rec.Code, http.StatusUnauthorized)
	}
	for _, name := range []string{
		"Access-Control-Allow-Origin",
		"Access-Control-Allow-Methods",
		"Access-Control-Allow-Headers",
		"Access-Control-Max-Age",
		"Access-Control-Expose-Headers",
	} {
		if got := rec.Header().Get(name); got != "" {
			t.Fatalf("WebDAV %s = %q, want absent", name, got)
		}
	}
	for _, dimension := range []string{"Origin", "Access-Control-Request-Method", "Access-Control-Request-Headers"} {
		if varyTokenCountForMainTest(rec.Header(), dimension) != 0 {
			t.Fatalf("WebDAV Vary = %#v, want no CORS dimension %s", rec.Header().Values("Vary"), dimension)
		}
	}
}

func varyTokenCountForMainTest(header http.Header, name string) int {
	count := 0
	for _, value := range header.Values("Vary") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), name) {
				count++
			}
		}
	}
	return count
}
