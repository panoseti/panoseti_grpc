import psutil

hashpipe_name = 'hashpipe'

def is_hashpipe_running(pid):
    # check pid first
    if psutil.pid_exists(pid):
        # then check if the process is a hashpipe process
        try:
            p = psutil.Process(pid)
            if hashpipe_name in p.cmdline():
                return True
            else:
                return False
        except:
            return False
    return False