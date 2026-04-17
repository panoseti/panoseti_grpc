import psutil

hashpipe_name = "hashpipe"


def is_hashpipe_running(pid: int) -> bool:
    # check pid first
    if psutil.pid_exists(pid):
        # then check if the process is a hashpipe process
        try:
            p = psutil.Process(pid)
            return hashpipe_name in p.cmdline()
        except Exception:
            return False
    return False
