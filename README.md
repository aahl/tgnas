<center><img alt="aahl/tgnas" src="https://github.com/user-attachments/assets/a810a605-a518-4adf-8497-83a2aeb50dc4" /></center>

# TgNAS

`tgnas` is an S3-compatible and WebDAV-capable gateway backed by Telegram storage and local SQLite metadata.

## Service modes

By default, `tgnas` reads `data/config.yaml` and starts one HTTP server with both protocols enabled:

```bash
tgnas
```

The S3 API is served at normal bucket paths. WebDAV is served under the configured WebDAV prefix, defaulting to `/dav/`.

Single-protocol modes are available when you only want one API surface:

```bash
tgnas -c config.yaml s3
tgnas -c config.yaml dav
```

`-c` is a short alias for `-config`. Passing both `-config` and `-c` in the same invocation is a usage error. `-debug` is a global flag, so it must appear before any subcommand.

## Configuration

The default config path is `data/config.yaml`.

Common environment variables:

- `TGNAS_LISTEN` overrides `server.listen` when `server.listen_env` is configured. Default is `:9000`.
- `TGNAS_SECRET_KEY` is the example S3/WebDAV credential secret.
- `TGNAS_TELEGRAM_BOT_TOKEN` provides the Telegram bot token.
- `TGNAS_TELEGRAM_CHAT_ID` is the default bucket chat ID reference.
- `TGNAS_SQLITE_PATH` can override the metadata SQLite path.

Bucket-level public read can be enabled for anonymous S3 object downloads:

```yaml
buckets:
  public-files:
    chat_id: "${TGNAS_TELEGRAM_CHAT_ID}"
    bot_token: "${TGNAS_XXXX_BOT_TOKEN}" # (Optional)
    public_read: true
```

`public_read` defaults to `false`. When enabled, anonymous S3 clients may only `GET` and `HEAD` objects in that bucket when they already know the object key. Bucket listing, root bucket listing, writes, deletes, and WebDAV still require authentication.

### S3 CORS

Browser access to the S3 API can be enabled globally with `server.allowed_origins` and overridden for individual buckets:

```yaml
server:
  allowed_origins:
    - "https://frontend.example.com"
    - "https://*.example.com"
    - '/^https://[a-z]+\.internal\.example$/'

buckets:
  photos:
    chat_id: "${TGNAS_TELEGRAM_CHAT_ID}"
    allowed_origins:
      - "https://photos.example.com"
```

Origin rules are case-insensitive and support exact values, `*` globs, and Go RE2 regular expressions wrapped in `/.../`. A non-empty bucket list replaces the global list. A missing, empty, or empty-string-only bucket list falls back to `server.allowed_origins`.

CORS is opt-in: when the effective origin list is empty, TgNAS preserves its existing S3 response and `OPTIONS` behavior. CORS applies only to the S3 API; WebDAV, `/healthz`, and `/readyz` are not decorated or short-circuited by the S3 CORS policy. CORS does not bypass SigV4 or `public_read` authentication rules.

Successful preflights allow `GET`, `HEAD`, `PUT`, `POST`, `DELETE`, and `OPTIONS`; accept `Authorization`, `Content-Type`, `Range`, `X-Amz-Date`, and `X-Amz-Content-Sha256`; and use a max age of 3600 seconds. Actual allowed-origin responses expose `ETag`, `Content-Range`, `Content-Length`, `Accept-Ranges`, and `Last-Modified`. TgNAS never emits `Access-Control-Allow-Credentials`.

Trusted proxy configuration for reverse proxies (e.g. cloudflared, nginx):

```yaml
server:
  trusted_proxies:
    - "127.0.0.1/32"
    - "172.16.0.0/12"
  trusted_proxy_hosts:
    - "s3.example.com"
```

When the request's remote IP matches a `trusted_proxies` CIDR range, `X-Forwarded-Host` (or `Forwarded: host=`) is trusted and applied to the request regardless of the host value. When the forwarded host matches a `trusted_proxy_hosts` entry (case-insensitive), the request is trusted regardless of the remote IP. Either match is sufficient. `X-Forwarded-Proto` (or `Forwarded: proto=`) is also applied when the request is trusted. Without trust, forwarded headers are ignored.

Warning: `trusted_proxy_hosts` trusts the forwarded host value itself, which is client-controlled unless a real proxy strips and sets it. Block direct untrusted access to TgNAS when using it. Prefer `trusted_proxies` CIDR ranges where possible.

WebDAV configuration:

```yaml
webdav:
  # prefix: "/dav/"
```

The prefix must start with `/`, is normalized to end with `/`, cannot be `/`, and cannot conflict with the first path segment of any configured bucket.

## Docker

Run it with the default Docker-oriented config in `data/config.yaml`:

```bash
mkdir -p data
wget -P data https://github.com/aahl/tgnas/raw/refs/heads/dev/data/config.yaml

docker run --rm -u root -v "$PWD/data:/app/data" ghcr.io/aahl/tgnas chown -R app:app /app/data

docker run -d \
  --name tgnas \
  -p 9000:9000 \
  -v "$PWD/data:/app/data" \
  -e TGNAS_SECRET_KEY="your-s3-and-webdav-password" \
  -e TGNAS_TELEGRAM_CHAT_ID="-1001234567890" \
  -e TGNAS_TELEGRAM_BOT_TOKEN="123456:telegram-bot-token" \
  ghcr.io/aahl/tgnas
```

The container runs as a non-root `app` user and uses `/app` as its working directory. The mounted `data` directory must be writable by that container user because SQLite metadata is stored under `/app/data` by default. If SQLite fails to open or create `metadata.sqlite`, fix the host directory ownership or permissions before restarting the container.

## Docker Compose

The included `docker-compose.yml` uses the published GHCR image and mounts `./data` to `/app/data`:

```bash
cat << EOF > .env
TGNAS_PORT_EXPOSED=9000
TGNAS_SECRET_KEY="your-s3-and-webdav-password"
TGNAS_TELEGRAM_CHAT_ID="-1001234567890"
TGNAS_TELEGRAM_BOT_TOKEN="123456:telegram-bot-token"
EOF

docker compose run --rm -u root tgnas chown -R app:app /app/data
wget -P data https://github.com/aahl/tgnas/raw/refs/heads/dev/data/config.yaml

docker compose up -d
```

If the host `data` directory is owned by root or another user, grant write access to the UID used by the container's `app` user, or use a permissions policy such as a writable group on `./data`. Do not make the config or SQLite directory read-only.

## Authentication

S3 keeps SigV4 authentication except for anonymous `GET` and `HEAD` object requests against buckets configured with `public_read: true`.

S3 object `GET` and `HEAD` requests may also use SigV4 query-string authentication, commonly called presigned URLs. Presigned URLs use the existing configured credentials and support `X-Amz-Expires` values up to `604800` seconds (7 days). Presigned URLs do not authorize bucket listing, root listing, writes, deletes, copy operations, or WebDAV requests.

A presigned object URL has the same path-style shape as normal S3 object access:

```text
https://s3.example.com/tgnas/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...&X-Amz-Date=...&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=...
```

If TgNAS is behind a reverse proxy that changes the origin host, configure `trusted_proxies` or `trusted_proxy_hosts` so the verifier sees the external host that was signed.

WebDAV uses HTTP Basic Auth and reuses `auth.credentials`:

- username: `access_key`
- password: resolved value of `secret_key_env`

## Local metadata CLI

`tgnas` also provides read-only local listing commands that inspect the configured SQLite metadata database. These commands do not start the HTTP server and do not contact Telegram.

```text
tgnas [-debug] [-c|-config config.yaml] ls [-n|-limit N] bucket[/prefix]
tgnas [-debug] [-c|-config config.yaml] lsd [bucket[/prefix]]
tgnas [-debug] [-c|-config config.yaml] bucket rename [--dry-run] old-bucket new-bucket
```

`ls` prints object keys, one per line. It defaults to 1000 results; `-limit N` and `-n N` set the maximum result count, and `0` means no overall result limit while still reading in pages internally.

`lsd` without a path prints enabled bucket names. `lsd bucket/prefix` prints direct pseudo-directories under the prefix using `/` as the delimiter.

`bucket rename` renames a bucket in the SQLite metadata database. The target bucket name must exist in the current config file with the same `chat_id` as the source bucket metadata. `--dry-run` prints what would change without modifying data. A warning is printed to stderr if the source bucket still appears in the config file.

## WebDAV behavior

WebDAV exposes object prefixes as directories. `MKCOL /dav/photos/2026/` creates a zero-byte directory marker object with key `2026/`, preserving empty directories.

Supported common operations include `OPTIONS`, `PROPFIND`, `GET`, `HEAD`, `PUT`, `DELETE`, `MKCOL`, `COPY`, and `MOVE`. `LOCK` and `UNLOCK` return not implemented, and `OPTIONS` does not advertise lock support.

`COPY` and `MOVE` are metadata-only within the same bucket, including recursive directory copy/move. They reuse existing Telegram file/chunk metadata rather than downloading and re-uploading content.

Buckets are still created from config only. If a bucket remains in metadata after being removed from config, it is treated as an orphan: normal object access is forbidden, but `DELETE /dav/{bucket}` or S3 `DELETE /{bucket}` can clean up the local metadata record and associated object/chunk metadata.

Bucket `chat_id` values can be literal Telegram chat IDs or full environment-variable references such as `chat_id: "${TGNAS_PRIVATE_CHAT_ID}"`. Partial interpolation is not supported. If the referenced environment variable is unset or empty, the resolved `chat_id` is empty and config validation fails.

## Links
- https://deepwiki.com/aahl/tgnas
- https://zread.ai/aahl/tgnas

<p align="center">
  <a href="https://linux.do/"><img alt="Linux.do community" src="https://img.shields.io/badge/Linux.do-community-0ea5e9?style=for-the-badge"></a>
</p>
