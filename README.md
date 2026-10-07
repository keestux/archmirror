# archmirror

Create and keep in sync a local copy of an Arch Linux package mirror over HTTP(S).

Official Arch mirrors are normally synced with `rsync`, but not every environment
has rsync access to a Tier-1/2 mirror. `archmirror.py` crawls a mirror's HTTP
directory listing instead and, like rsync, only transfers what changed since the
last run. Run it regularly (e.g. from cron) to keep the mirror current.

## Features

- **Incremental sync** — files already present locally with the same size (and
  modification time, when the server sends `Last-Modified`) are skipped.
- **Safe downloads** — files are written to a `.part` temp file and atomically
  renamed into place, so an interrupted run never leaves a corrupt file behind.
- **Concurrent** — downloads run in a thread pool for throughput.
- **`--delete`** — remove local files and directories that no longer exist
  upstream, like `rsync --delete`.
- **`--touch-lastsync`** — write a `lastsync` file at the destination root after
  a fully successful run, the convention used to check mirror freshness.
- **`--verify-checksums`** — verify every package (existing and newly downloaded)
  against the sha256 sums in each repo's `.db` file. Mismatched files are removed
  so the next run retries them.
- **Locking** — a `flock(2)` lock prevents overlapping runs against the same
  destination. It is released automatically if a run crashes or is killed.

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) — dependencies (`requests`, `zstandard`,
  `click`) are declared inline (PEP 723) and installed automatically on first run.
- A POSIX system (Linux, macOS, BSD); the lock uses `fcntl.flock()`.

Install uv if you don't have it:

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Usage

```sh
chmod +x archmirror.py
./archmirror.py --dest /srv/archmirror

# or, equivalently
uv run archmirror.py --dest /srv/archmirror
```

A typical full invocation (the first run makes a full copy; later runs are
incremental). Note that `--repos` is repeated per repo:

```sh
./archmirror.py \
    --source https://geo.mirror.pkgbuild.com \
    --dest /srv/archmirror \
    --repos core --repos extra --repos multilib \
    --arch x86_64 \
    --delete --touch-lastsync
```

### Options

| Option | Default | Description |
| --- | --- | --- |
| `--source URL` | `https://geo.mirror.pkgbuild.com` | Base URL of the upstream mirror. |
| `--dest DIR` | *(required)* | Local destination directory. |
| `--repos NAME` | `core`, `extra`, `multilib` | Repo to sync. Repeat for multiple. |
| `--arch ARCH` | `x86_64` | Architecture to sync. |
| `--include-iso` | off | Also sync the `iso/` directory (large install images). |
| `--workers N` | `8` | Concurrent download threads. |
| `--timeout SEC` | `30` | Per-request timeout in seconds. |
| `--retries N` | `3` | HTTP retry attempts. |
| `--delete` | off | Delete local files/dirs no longer present upstream. |
| `--dry-run` | off | Show what would happen, change nothing. |
| `--touch-lastsync` | off | Write a `lastsync` file after a fully successful run. |
| `--verify-checksums` | off | Verify packages against sha256 sums in each repo's `.db`. |
| `--lock-file PATH` | `<dest>/.archmirror.lock` | Lock file used to prevent parallel runs. |
| `--wait-for-lock` | off | Wait for a running sync to finish instead of exiting. |
| `-v`, `--verbose` | off | Debug logging. |
| `-h`, `--help` | | Show help and exit. |

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success. |
| `1` | One or more files failed to sync or verify. |
| `2` | Another run already holds the lock (only without `--wait-for-lock`). |
| `130` | Interrupted with Ctrl-C. |

### Interrupting

Ctrl-C stops all download threads. Queued downloads are cancelled, and running
ones stop and remove their `.part` file. Any thread still stuck waiting on the
network after 5 seconds is killed by exiting the process. Press Ctrl-C a second
time to skip the wait.

### Running from cron

Example crontab entry syncing every 3 hours:

```cron
0 */3 * * * /opt/archmirror/archmirror.py --source https://geo.mirror.pkgbuild.com --dest /srv/archmirror --repos core --repos extra --repos multilib --arch x86_64 --delete --touch-lastsync >> /var/log/archmirror.log 2>&1
```

If a run is still going when the next one is due, the new run exits with code 2
rather than syncing in parallel.

## Limitations

- Without `--verify-checksums`, only size (and mtime when available) is compared.
  This is fast and normally sufficient; checksum verification is slower because
  every file has to be hashed.
- Checksum verification covers only packages listed in the repo database —
  not `*.sig` files, the db/files archives themselves, or anything under `iso/`.
- `iso/` is skipped by default; use `--include-iso` to fetch it.
- Locking is POSIX-only and won't work on Windows as-is.
