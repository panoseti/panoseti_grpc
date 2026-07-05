import glob
import os

import psutil

hashpipe_name = "hashpipe"

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
