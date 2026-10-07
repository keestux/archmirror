#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests",
#     "zstandard",
#     "click",
# ]
# ///
"""
archmirror.py - Create and keep in sync a local copy of an Arch Linux
package mirror over HTTP(S).

Why this exists
----------------
Official Arch Linux mirrors are normally synced with `rsync`, but not every
environment has rsync access to a Tier-1/2 mirror, or you may simply want a
self-contained Python tool. This script walks a mirror's HTTP directory
listing and downloads files, but - like rsync - it skips anything that is
already present locally with the same size (and, when available, the same
modification time). Re-running it regularly (e.g. from cron) will only
transfer what changed since the last run.

What it does
------------
1. Recursively crawls the directory index pages of the given repos/arches
   (e.g. core/os/x86_64, extra/os/x86_64, multilib/os/x86_64).
2. For every file found remotely, issues a lightweight HEAD request to get
   the authoritative size (and Last-Modified) and compares it against the
   local file before deciding whether to download.
3. Downloads happen to a `.part` temp file and are atomically renamed into
   place, so an interrupted run never leaves a corrupt "final" file behind.
4. Downloads are done concurrently (thread pool) for throughput.
5. Optional --delete removes local files/directories that no longer exist
   on the remote side (like `rsync --delete`), so the mirror doesn't grow
   stale packages forever.
6. Optional --touch-lastsync writes a `lastsync` file at the destination
   root after a fully successful run, which is the convention downstream
   mirrors/clients use to check mirror freshness.
7. Optional --verify-checksums downloads each repo's `.db` file (e.g.
   core.db), which is a compressed tar containing a `desc` entry per
   package with its expected sha256sum, and verifies every package
   against it - both files that already exist locally (to catch silent
   corruption) and files just downloaded (to catch transfer errors).
   Mismatches are logged, the bad local file is removed, and the run
   exits with a non-zero status so the next scheduled run retries it.
8. A lock file (default: `.archmirror.lock` inside --dest) is held for
   the whole run using flock(2), so if a scheduled run is still going
   when the next one is due to start, the new one refuses to start
   syncing in parallel against the same destination - by default it
   logs an error and exits immediately (exit code 2); pass
   --wait-for-lock to have it queue up and start once the first run
   finishes instead. The lock is released automatically if a run
   crashes or is killed, so it can never get stuck "held" forever.

Exit codes: 0 = success, 1 = one or more files failed to sync/verify,
2 = another run already held the lock (only possible without
--wait-for-lock), 130 = interrupted with Ctrl-C.

Interrupting (Ctrl-C) stops all download threads: queued downloads are
cancelled, running ones notice within one chunk and remove their `.part`
file, and any thread still blocked in network I/O after a short grace
period is killed by exiting the process. A second Ctrl-C skips the grace
period.

Requirements
------------
Uses uv (https://docs.astral.sh/uv/) to manage dependencies - no manual
virtualenv or pip install needed. Dependencies are declared inline above
(PEP 723) and uv resolves/installs them into an isolated environment the
first time the script runs.

    # one-off install of uv itself, if you don't have it:
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # make it executable and run directly (uses the shebang above):
    chmod +x archmirror.py
    ./archmirror.py --dest /srv/archmirror

    # ...or explicitly via `uv run`, which works the same either way:
    uv run archmirror.py --dest /srv/archmirror

Example usage
-------------
    # First run (full copy) and every subsequent incremental run.
    # Note --repos is repeated per repo (click option), not space-separated:
    ./archmirror.py \\
        --source https://geo.mirror.pkgbuild.com \\
        --dest /srv/archmirror \\
        --repos core --repos extra --repos multilib \\
        --arch x86_64 \\
        --delete --touch-lastsync

Example crontab entry (every 3 hours):
    0 */3 * * * /opt/archmirror/archmirror.py \\
        --source https://geo.mirror.pkgbuild.com --dest /srv/archmirror \\
        --repos core --repos extra --repos multilib --arch x86_64 \\
        --delete --touch-lastsync >> /var/log/archmirror.log 2>&1

Notes / limitations
--------------------
- Without --verify-checksums this only compares size (and mtime when the
  server sends Last-Modified) - fast, and normally enough. Pass
  --verify-checksums for cryptographic verification against the repo
  database instead (slower, since every file has to be hashed).
- Checksum verification only covers files listed in the repo database
  (the actual packages). Signature files (*.sig), the db/files archives
  themselves, and anything in iso/ are not covered by it.
- The "iso/" directory (installation images) is skipped by default since
  it is large and not needed by pacman clients; pass --include-iso to
  fetch it too.
- The lock uses fcntl.flock(), which is POSIX-only (Linux/macOS/BSD).
  It won't work as-is on Windows.
"""

from __future__ import annotations

import contextlib
import email.utils
import fcntl
import gzip
import hashlib
import io
import logging
import os
import re
import shutil
import subprocess
import sys
import tarfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from pathlib import Path
from typing import NoReturn
from urllib.parse import urljoin, unquote, quote

import click
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import zstandard  # type: ignore
except ImportError:
    zstandard = None

LOG = logging.getLogger("archmirror")

DEFAULT_REPOS = ["core", "extra", "multilib"]
DEFAULT_ARCH = "x86_64"

# Matches href="..." attributes in a standard Apache/nginx autoindex page.
HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)

USER_AGENT = "archmirror.py/1.0 (+python-requests)"

# Seconds to let running download threads wind down after Ctrl-C before
# the process is force-exited (killing any thread still blocked in I/O).
SHUTDOWN_GRACE = 5.0

# Set on Ctrl-C; worker threads poll it and bail out via SyncCancelled.
_stop = threading.Event()

# .part files currently being written, so _abort() can remove the ones
# belonging to threads it has to kill.
_active_parts: set[Path] = set()


class SyncCancelled(Exception):
    """Raised inside a worker thread when the run is being interrupted."""


def _check_stop() -> None:
    if _stop.is_set():
        raise SyncCancelled()


class Stats:
    def __init__(self) -> None:
        self.checked = 0
        self.downloaded = 0
        self.skipped = 0
        self.deleted = 0
        self.bytes_downloaded = 0
        self.errors = 0
        self.verified = 0
        self.checksum_mismatches = 0

    def summary(self) -> str:
        mb = self.bytes_downloaded / (1024 * 1024)
        return (
            f"checked={self.checked} downloaded={self.downloaded} "
            f"skipped={self.skipped} deleted={self.deleted} "
            f"downloaded_MB={mb:.1f} verified={self.verified} "
            f"checksum_mismatches={self.checksum_mismatches} errors={self.errors}"
        )


def build_session(retries: int, timeout: int) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(
        total=retries,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.request_timeout = timeout  # stashed for convenience
    return session


def fetch_listing(session: requests.Session, url: str, timeout: int) -> list[tuple[str, bool]]:
    """Return [(name, is_dir), ...] for a directory index page."""
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    entries: list[tuple[str, bool]] = []
    seen = set()
    for href in HREF_RE.findall(resp.text):
        if href.startswith("?"):
            continue
        if href in ("../", "./"):
            continue
        if href.startswith("http://") or href.startswith("https://"):
            continue  # absolute link to elsewhere, not a listing entry
        if href.startswith("/"):
            continue
        name = unquote(href)
        if name in seen:
            continue
        seen.add(name)
        is_dir = name.endswith("/")
        entries.append((name.rstrip("/") if is_dir else name, is_dir))
    return entries


def parse_last_modified(headers: dict) -> float | None:
    raw = headers.get("Last-Modified")
    if not raw:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        return dt.timestamp()
    except (TypeError, ValueError):
        return None


def files_match(local_path: Path, remote_size: int | None, remote_mtime: float | None) -> bool:
    if not local_path.exists():
        return False
    st = local_path.stat()
    if remote_size is not None and st.st_size != remote_size:
        return False
    if remote_mtime is not None:
        # Allow a couple of seconds of slack; local file must be at least
        # as new as the remote one to be considered in sync.
        if st.st_mtime < remote_mtime - 2:
            return False
    return True


def decompress_zstd(data: bytes) -> bytes:
    """Decompress zstd-compressed bytes, using the `zstandard` package if
    installed, otherwise falling back to the system `zstd`/`unzstd` CLI."""
    if zstandard is not None:
        return zstandard.ZstdDecompressor().decompress(data, max_output_size=1024 * 1024 * 1024)

    for exe, cli_args in (("zstd", ["-d", "-c", "-q"]), ("unzstd", ["-c", "-q"])):
        path = shutil.which(exe)
        if path:
            proc = subprocess.run([path, *cli_args], input=data, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
            if proc.returncode == 0:
                return proc.stdout
            LOG.debug("%s failed: %s", exe, proc.stderr.decode(errors="replace"))

    raise RuntimeError(
        "cannot decompress zstd data: install the 'zstandard' Python package "
        "(pip install zstandard) or the 'zstd' command-line tool"
    )


def decompress_auto(data: bytes) -> bytes:
    """Decompress a repo .db file's bytes regardless of whether it's
    gzip- or zstd-compressed (Arch has used both over time), or return it
    unchanged if it is already a plain tar."""
    if data[:2] == b"\x1f\x8b":
        return gzip.decompress(data)
    if data[:4] == b"\x28\xb5\x2f\xfd":
        return decompress_zstd(data)
    if data[257:262] == b"ustar":
        return data  # already an uncompressed tar
    raise ValueError("unrecognized repo database compression (not gzip, zstd, or plain tar)")


def parse_db(tar_bytes: bytes) -> dict[str, str]:
    """Parse a decompressed repo database tar and return {filename: sha256sum}."""
    checksums: dict[str, str] = {}
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as tar:
        for member in tar.getmembers():
            if not member.isfile() or not member.name.endswith("/desc"):
                continue
            fh = tar.extractfile(member)
            if fh is None:
                continue
            content = fh.read().decode("utf-8", errors="replace")

            fields: dict[str, list[str]] = {}
            key = None
            for line in content.splitlines():
                if line.startswith("%") and line.endswith("%"):
                    key = line.strip("%")
                    fields[key] = []
                elif line.strip() == "":
                    key = None
                elif key is not None:
                    fields[key].append(line)

            filename = fields.get("FILENAME", [None])[0]
            sha256sum = fields.get("SHA256SUM", [None])[0]
            if filename and sha256sum:
                checksums[filename] = sha256sum.lower()
    return checksums


def join_url(base_dir_url: str, segment: str) -> str:
    """Join a base directory URL (must end with '/') with a single path
    segment (one file or directory name), percent-encoding it ourselves.

    Deliberately does NOT use urljoin()/urlsplit(): Arch package filenames
    with an epoch look like 'fakeroot-1:1.37.2-3-x86_64.pkg.tar.zst', and
    urlsplit() treats the 'fakeroot-1' before the colon as a URI scheme,
    turning the "joined" result into an unchanged, bogus absolute URL
    (requests then fails with "No connection adapters were found"). Simple
    string concatenation of a percent-encoded segment sidesteps that
    entirely."""
    assert base_dir_url.endswith("/"), base_dir_url
    return base_dir_url + quote(segment, safe="")


def fetch_repo_checksums(
    session: requests.Session, remote_dir_url: str, repo_name: str, timeout: int
) -> dict[str, str]:
    """Fetch and parse '<repo_name>.db' from remote_dir_url, returning a
    {filename: sha256sum} map. Returns {} (with a logged warning) on any
    failure so mirroring can proceed without verification rather than
    aborting entirely."""
    db_url = join_url(remote_dir_url, f"{repo_name}.db")
    try:
        resp = session.get(db_url, timeout=timeout)
        resp.raise_for_status()
        tar_bytes = decompress_auto(resp.content)
        checksums = parse_db(tar_bytes)
        LOG.info("loaded %d checksums from %s", len(checksums), db_url)
        return checksums
    except (requests.RequestException, tarfile.TarError, ValueError, RuntimeError) as exc:
        LOG.warning("could not load repo database %s for verification: %s", db_url, exc)
        return {}


def compute_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            _check_stop()
            digest.update(chunk)
    return digest.hexdigest()


def sync_file(
    session: requests.Session,
    remote_url: str,
    local_path: Path,
    timeout: int,
    dry_run: bool,
    stats: Stats,
    expected_sha256: str | None = None,
) -> None:
    _check_stop()
    stats.checked += 1
    try:
        head = session.head(remote_url, timeout=timeout, allow_redirects=True)
        head.raise_for_status()
    except requests.RequestException as exc:
        LOG.error("HEAD failed for %s: %s", remote_url, exc)
        stats.errors += 1
        return
    _check_stop()

    remote_size = None
    if "Content-Length" in head.headers:
        try:
            remote_size = int(head.headers["Content-Length"])
        except ValueError:
            remote_size = None
    remote_mtime = parse_last_modified(head.headers)

    up_to_date = files_match(local_path, remote_size, remote_mtime)
    if up_to_date and expected_sha256:
        actual = compute_sha256(local_path)
        if actual != expected_sha256:
            LOG.warning(
                "checksum mismatch for existing file %s (expected %s, got %s) - re-downloading",
                local_path, expected_sha256, actual,
            )
            stats.checksum_mismatches += 1
            up_to_date = False
        else:
            stats.verified += 1

    if up_to_date:
        LOG.debug("up to date: %s", local_path)
        stats.skipped += 1
        return

    if dry_run:
        LOG.info("[dry-run] would download %s -> %s", remote_url, local_path)
        stats.downloaded += 1
        return

    local_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = local_path.with_suffix(local_path.suffix + ".part")

    _active_parts.add(tmp_path)
    try:
        with session.get(remote_url, stream=True, timeout=timeout) as resp:
            resp.raise_for_status()
            written = 0
            with open(tmp_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    _check_stop()
                    if chunk:
                        fh.write(chunk)
                        written += len(chunk)
        os.replace(tmp_path, local_path)
        if remote_mtime is not None:
            os.utime(local_path, (remote_mtime, remote_mtime))

        if expected_sha256:
            actual = compute_sha256(local_path)
            if actual != expected_sha256:
                LOG.error(
                    "checksum mismatch after download for %s (expected %s, got %s) - removing",
                    local_path, expected_sha256, actual,
                )
                stats.checksum_mismatches += 1
                stats.errors += 1
                local_path.unlink(missing_ok=True)
                return
            stats.verified += 1

        stats.downloaded += 1
        stats.bytes_downloaded += written
        LOG.info("downloaded %s (%.1f KB)", local_path, written / 1024)
    except requests.RequestException as exc:
        LOG.error("download failed for %s: %s", remote_url, exc)
        stats.errors += 1
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
    except SyncCancelled:
        tmp_path.unlink(missing_ok=True)
        raise
    finally:
        _active_parts.discard(tmp_path)


def sync_directory(
    session: requests.Session,
    remote_url: str,
    local_dir: Path,
    executor: ThreadPoolExecutor,
    timeout: int,
    dry_run: bool,
    delete: bool,
    stats: Stats,
    futures: list,
    checksum_map: dict[str, str] | None = None,
) -> None:
    """Recursively mirror remote_url into local_dir. File downloads are
    submitted to the shared executor; directory traversal itself is
    sequential (cheap - only listing pages, not package downloads).

    checksum_map, if given, maps filename -> expected sha256sum (as loaded
    from a repo .db by fetch_repo_checksums) and is used to verify files in
    this directory. It intentionally does NOT get passed into subdirectory
    recursion, since repo databases only describe the flat package
    directory they live in, not nested subdirectories."""
    if not remote_url.endswith("/"):
        remote_url += "/"
    local_dir.mkdir(parents=True, exist_ok=True)

    try:
        entries = fetch_listing(session, remote_url, timeout)
    except requests.RequestException as exc:
        LOG.error("failed to list %s: %s", remote_url, exc)
        stats.errors += 1
        return

    remote_names = set()
    for name, is_dir in entries:
        remote_names.add(name)
        if is_dir:
            sync_directory(
                session,
                join_url(remote_url, name) + "/",
                local_dir / name,
                executor,
                timeout,
                dry_run,
                delete,
                stats,
                futures,
            )
        else:
            file_url = join_url(remote_url, name)
            local_path = local_dir / name
            expected_sha256 = checksum_map.get(name) if checksum_map else None
            futures.append(
                executor.submit(
                    sync_file, session, file_url, local_path, timeout, dry_run, stats,
                    expected_sha256,
                )
            )

    if delete and local_dir.exists():
        for entry in local_dir.iterdir():
            if entry.name not in remote_names and not entry.name.endswith(".part"):
                if dry_run:
                    LOG.info("[dry-run] would delete %s", entry)
                    stats.deleted += 1
                    continue
                try:
                    if entry.is_dir():
                        _rmtree(entry)
                    else:
                        entry.unlink()
                    LOG.info("deleted %s", entry)
                    stats.deleted += 1
                except OSError as exc:
                    LOG.error("failed to delete %s: %s", entry, exc)
                    stats.errors += 1


class LockHeldError(Exception):
    """Raised when another archmirror process already holds the lock file."""


@contextlib.contextmanager
def acquire_lock(lock_path: Path, wait: bool):
    """Take an exclusive, non-blocking (unless wait=True) lock on
    lock_path for the duration of the `with` block, so two archmirror runs
    against the same destination never sync in parallel (which could
    corrupt files being written, race on --delete, etc). Uses flock(2),
    which is released automatically if the process dies, so a stale lock
    can never survive a crash or a killed process.

    Raises LockHeldError immediately if wait=False and another process
    already holds the lock; blocks until it's free if wait=True."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "w")
    try:
        flags = fcntl.LOCK_EX if wait else fcntl.LOCK_EX | fcntl.LOCK_NB
        try:
            fcntl.flock(fh, flags)
        except BlockingIOError:
            raise LockHeldError(lock_path)
        fh.write(f"{os.getpid()}\n")
        fh.flush()
        yield
    finally:
        try:
            fcntl.flock(fh, fcntl.LOCK_UN)
        finally:
            fh.close()


def _abort(executor: ThreadPoolExecutor, futures: list) -> NoReturn:
    """Stop all download threads after Ctrl-C and exit with status 130.

    Threads can't be killed from outside in Python, so: signal them via
    _stop (they check it between chunks and remove their .part file),
    cancel everything still queued, and give running threads
    SHUTDOWN_GRACE seconds to finish. Any thread still blocked in network
    I/O after that is killed by force-exiting the process (os._exit), which
    also releases the flock. A second Ctrl-C skips the grace period."""
    _stop.set()
    executor.shutdown(wait=False, cancel_futures=True)
    running = [f for f in futures if not f.done()]
    LOG.warning("interrupted - stopping %d running download thread(s)", len(running))
    try:
        wait(running, timeout=SHUTDOWN_GRACE)
    except KeyboardInterrupt:
        pass
    stuck = [f for f in running if not f.done()]
    if stuck:
        LOG.warning("killing %d thread(s) still blocked in network I/O", len(stuck))
        for tmp_path in list(_active_parts):
            tmp_path.unlink(missing_ok=True)
        logging.shutdown()
        os._exit(130)
    sys.exit(130)


def _rmtree(path: Path) -> None:
    for child in path.iterdir():
        if child.is_dir():
            _rmtree(child)
        else:
            child.unlink()
    path.rmdir()


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--source", default="https://geo.mirror.pkgbuild.com", show_default=True,
              help="Base URL of the upstream mirror.")
@click.option("--dest", required=True, type=click.Path(file_okay=False, path_type=Path),
              help="Local destination directory.")
@click.option("--repos", multiple=True, default=DEFAULT_REPOS, show_default=True,
              help="Repo to sync. Repeat for multiple, e.g. --repos core --repos extra.")
@click.option("--arch", default=DEFAULT_ARCH, show_default=True, help="Architecture to sync.")
@click.option("--include-iso", is_flag=True, help="Also sync the iso/ directory (large install images).")
@click.option("--workers", type=int, default=8, show_default=True, help="Concurrent download threads.")
@click.option("--timeout", type=int, default=30, show_default=True, help="Per-request timeout in seconds.")
@click.option("--retries", type=int, default=3, show_default=True, help="HTTP retry attempts.")
@click.option("--delete", is_flag=True, help="Delete local files/dirs no longer present on the remote.")
@click.option("--dry-run", is_flag=True, help="Show what would happen, change nothing.")
@click.option("--touch-lastsync", is_flag=True,
              help="Write a 'lastsync' file at --dest after a fully successful run.")
@click.option("--verify-checksums", is_flag=True,
              help="Verify packages against sha256sums in each repo's .db file "
                   "(both existing local files and new downloads).")
@click.option("--lock-file", type=click.Path(dir_okay=False, path_type=Path), default=None,
              help="Lock file path used to prevent two runs against the same destination "
                   "from syncing in parallel. Defaults to '.archmirror.lock' inside --dest.")
@click.option("--wait-for-lock", is_flag=True,
              help="If another run holds the lock, wait for it to finish instead of exiting "
                   "immediately. Without this, a second overlapping run exits right away.")
@click.option("-v", "--verbose", is_flag=True, help="Verbose (debug) logging.")
def main(
    source: str,
    dest: Path,
    repos: tuple[str, ...],
    arch: str,
    include_iso: bool,
    workers: int,
    timeout: int,
    retries: int,
    delete: bool,
    dry_run: bool,
    touch_lastsync: bool,
    verify_checksums: bool,
    lock_file: Path | None,
    wait_for_lock: bool,
    verbose: bool,
) -> None:
    """Mirror an Arch Linux repository over HTTP(S), rsync-style."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    dest_root = dest.expanduser().resolve()
    dest_root.mkdir(parents=True, exist_ok=True)
    lock_path = lock_file.expanduser().resolve() if lock_file else dest_root / ".archmirror.lock"

    try:
        with acquire_lock(lock_path, wait=wait_for_lock):
            _run_sync(
                source, dest_root, repos, arch, include_iso, workers, timeout,
                retries, delete, dry_run, touch_lastsync, verify_checksums,
            )
    except LockHeldError:
        LOG.error(
            "another archmirror run already holds the lock (%s) - exiting "
            "(pass --wait-for-lock to wait instead)", lock_path,
        )
        sys.exit(2)
    except KeyboardInterrupt:
        # Interrupted outside the download phase (e.g. waiting for the lock).
        LOG.warning("interrupted")
        sys.exit(130)


def _run_sync(
    source: str,
    dest_root: Path,
    repos: tuple[str, ...],
    arch: str,
    include_iso: bool,
    workers: int,
    timeout: int,
    retries: int,
    delete: bool,
    dry_run: bool,
    touch_lastsync: bool,
    verify_checksums: bool,
) -> None:
    """The actual sync, run while the lock in main() is held."""
    session = build_session(retries, timeout)
    stats = Stats()
    start = time.time()

    # (relative_path, repo_name_or_None) - repo_name is used to look up
    # "<repo_name>.db" for checksum verification; iso/ has no db file.
    targets = [(f"{repo}/os/{arch}", repo) for repo in repos]
    if include_iso:
        targets.append(("iso", None))

    # Not a `with` block: its implicit shutdown(wait=True) would block on
    # Ctrl-C until every queued download had finished.
    executor = ThreadPoolExecutor(max_workers=workers)
    futures: list = []
    try:
        for rel_path, repo_name in targets:
            remote_url = urljoin(source.rstrip("/") + "/", rel_path + "/")
            local_dir = dest_root / rel_path
            LOG.info("syncing %s -> %s", remote_url, local_dir)

            checksum_map = None
            if verify_checksums and repo_name:
                checksum_map = fetch_repo_checksums(session, remote_url, repo_name, timeout)

            sync_directory(
                session, remote_url, local_dir, executor,
                timeout, dry_run, delete, stats, futures,
                checksum_map,
            )
        for fut in as_completed(futures):
            fut.result()  # re-raise unexpected exceptions, if any
    except KeyboardInterrupt:
        _abort(executor, futures)
    executor.shutdown(wait=True)

    elapsed = time.time() - start
    LOG.info("done in %.1fs - %s", elapsed, stats.summary())

    if touch_lastsync and not dry_run and stats.errors == 0:
        (dest_root / "lastsync").write_text(str(int(time.time())))
        LOG.info("wrote lastsync marker")

    sys.exit(1 if stats.errors else 0)


if __name__ == "__main__":
    main()
