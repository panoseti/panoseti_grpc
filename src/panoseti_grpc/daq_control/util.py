import ctypes
import ctypes.util
import glob
import os

import psutil

hashpipe_name = "hashpipe"

# hashpipe_status.h: HASHPIPE_STATUS_TOTAL_SIZE (2880*64), 80-byte FITS-style
# "card" records, buffer stored in SysV shared memory keyed by ftok(keyfile,
# proj_id) where keyfile is $HASHPIPE_KEYFILE or $HOME or "/tmp", and proj_id
# for the *status* buffer specifically is (instance_id & 0x3f) | 0x40 (the
# databuf uses | 0x80 instead -- same keyfile, different proj_id, so they
# never collide). See hashpipe_ipckey.c / hashpipe_status.c upstream.
_STATUS_BUFFER_SIZE = 2880 * 64
_STATUS_CARD_SIZE = 80
_STATUS_PROJ_ID_MASK = 0x40

_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
_libc.ftok.restype = ctypes.c_int
_libc.ftok.argtypes = [ctypes.c_char_p, ctypes.c_int]
_libc.shmget.restype = ctypes.c_int
_libc.shmget.argtypes = [ctypes.c_int, ctypes.c_size_t, ctypes.c_int]
_libc.shmat.restype = ctypes.c_void_p
_libc.shmat.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
_libc.shmdt.restype = ctypes.c_int
_libc.shmdt.argtypes = [ctypes.c_void_p]


def _hashpipe_status_key(instance_id: int) -> int:
    """Reproduce hashpipe_status_key() from hashpipe_ipckey.c."""
    keyfile = os.environ.get("HASHPIPE_KEYFILE") or os.environ.get("HOME") or "/tmp"
    proj_id = (instance_id & 0x3F) | _STATUS_PROJ_ID_MASK
    key = int(_libc.ftok(keyfile.encode(), proj_id))
    if key == -1:
        errno = ctypes.get_errno()
        raise OSError(errno, f"ftok({keyfile!r}, {proj_id}) failed: {os.strerror(errno)}")
    return key


def _parse_status_buffer(raw: bytes) -> dict[str, str]:
    """Parse hashpipe's FITS-style 80-byte-card status buffer into a dict.

    Each card looks like ``KEYWORD = value / optional comment``, padded with
    spaces to 80 bytes. Parsing stops at the "END" card (the rest is unused
    buffer space, not zeroed).
    """
    result: dict[str, str] = {}
    for i in range(0, len(raw), _STATUS_CARD_SIZE):
        card = raw[i : i + _STATUS_CARD_SIZE]
        text = card.decode("ascii", errors="replace").rstrip()
        if not text or text.startswith("END"):
            break
        if "=" not in text:
            continue
        key, _, rest = text.partition("=")
        key = key.strip()
        value = rest.split("/", 1)[0].strip()
        if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
            value = value[1:-1].strip()
        if key:
            result[key] = value
    return result


def read_hashpipe_status_buffer(instance_id: int = 0) -> dict[str, str]:
    """Read hashpipe's live status shared-memory buffer for *instance_id*.

    This is the same FITS-style buffer net_thread/compute_thread/
    output_thread write per-thread state and packet counters into (NETSTAT,
    NETDROPS, NPACKETS, TPKTLST, COMSTAT, OUTSTAT, ...) -- richer than the
    thread-count health check, but only meaningful while hashpipe is
    actually running and has attached to (and initialized) this buffer.

    Returns an empty dict if the buffer doesn't exist (no hashpipe has ever
    attached for this instance_id) or can't be read -- this is a best-effort
    read for status display, not a correctness-critical path, so callers
    should treat an empty dict as "no data available" rather than an error.
    """
    try:
        key = _hashpipe_status_key(instance_id)
        # 0o666, no IPC_CREAT: attach only if it already exists -- creating
        # a fresh (zeroed, uninitialized) segment would be misleading here.
        shmid = _libc.shmget(key, _STATUS_BUFFER_SIZE, 0o666)
        if shmid == -1:
            return {}
        addr = _libc.shmat(shmid, None, 0)
        if ctypes.cast(addr, ctypes.c_void_p).value is None or addr == ctypes.c_void_p(-1).value:
            return {}
        try:
            raw = ctypes.string_at(addr, _STATUS_BUFFER_SIZE)
        finally:
            _libc.shmdt(addr)
        return _parse_status_buffer(raw)
    except Exception:
        return {}

# hashpipe's pipeline is 1 main thread (blocked in pthread_join once it has
# spawned the workers) + net_thread + compute_thread + output_thread. Fewer
# than this while the process is alive means it's stuck mid-init (e.g.
# blocked acquiring a stale semaphore in hashpipe_databuf_create) or a
# worker thread has died -- either way, no science data is flowing.
EXPECTED_HASHPIPE_THREADS = 4


def is_hashpipe_running(pid: int, name: str = "hashpipe") -> bool:
    # check pid first
    if psutil.pid_exists(pid):
        # then check if the process is a hashpipe process
        try:
            p = psutil.Process(pid)
            # Check for substring in name OR any command line argument
            # to support shebang scripts and various execution paths.
            p_name = p.name() or ""
            p_cmdline = p.cmdline() or []
            return name.lower() in p_name.lower() or any(name.lower() in arg.lower() for arg in p_cmdline)
        except Exception:
            return False
    return False


def hashpipe_thread_count(pid: int) -> int:
    """Number of live threads for *pid*, or 0 if the process is gone."""
    try:
        return int(psutil.Process(pid).num_threads())
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return 0


def cleanup_stale_hashpipe_semaphores(instance_id: int = 0) -> list[str]:
    """Remove leaked hashpipe POSIX semaphores for *instance_id*.

    hashpipe's status-buffer semaphore is named by instance ID, not PID, so
    if a prior process was killed (or its container force-recreated) while
    holding it, every subsequent hashpipe process blocks forever inside
    hashpipe_databuf_create() during shared-memory init -- before it ever
    spawns net_thread/compute_thread/output_thread, and with no error or
    crash to observe. Only call this once the caller has verified no
    hashpipe process is currently running: a live process legitimately
    holds this semaphore.
    """
    removed = []
    for path in glob.glob(f"/dev/shm/sem.*hashpipe_status_{instance_id}"):
        try:
            os.remove(path)
            removed.append(path)
        except OSError:
            pass
    return removed
