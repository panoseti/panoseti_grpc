import psutil

hashpipe_name = "hashpipe"


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
            return (name.lower() in p_name.lower() or 
                    any(name.lower() in arg.lower() for arg in p_cmdline))
        except Exception:
            return False
    return False
