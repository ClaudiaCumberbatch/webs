import psutil
import os
import signal
import random

def dependency():
    def fails():
        raise ValueError("Deliberate failure")

    def depends(parent):
        return 1
    
    f1 = fails()
    f2 = depends(f1)
    f3 = depends(f2)
    f4 = depends(f3)
    print(f1.result(), f2.result(), f3.result(), f4.result())

def divide_zero():
    return 100/0

def environment():
    import non_exist

def manager_kill():
    # kill father process
    current_pid = os.getpid()
    current_process = psutil.Process(current_pid)
    parent_process = current_process.parent()

    if parent_process:
        parent_pid = parent_process.pid
        print(f"Killing Manager with PID: {parent_pid}")
        
        try:
            os.kill(parent_pid, signal.SIGTERM)
            print(f"Parent process {parent_pid} terminated.")
        except psutil.NoSuchProcess:
            print(f"Parent process {parent_pid} does not exist.")
        except psutil.AccessDenied:
            print(f"No permission to terminate parent process {parent_pid}.")
    else:
        print("No parent process found.")

def memory():
    huge_memory_list = []
    while True:
        huge_memory_list.append('A' * 1024 * 1024 * 100)

def node_kill():
    current_pid = os.getpid()
    
    for proc in psutil.process_iter(attrs=['pid', 'name']):
        pid = proc.info['pid']
        if pid == current_pid:
            continue
        try:
            p = psutil.Process(pid)
            p.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            print(f"raise exception {e} in node kill")
            pass
    
    psutil.wait_procs(psutil.process_iter(), timeout=3, callback=None)
    print("node killed")


def ulimit():
    # limit = 10 
    limit = 548001
    handles = []
    try:
        for i in range(limit):
            handles.append(open(f"/tmp/tempfile_{i}.txt", "w"))
        return f"Opened {limit} files successfully"
    finally:
        for handle in handles:
            handle.close()

def walltime():
    import time
    while True:
        time.sleep(60)

def worker_kill():
    current_pid = os.getpid()
    parent_pid = os.getppid()

    all_processes_pid = []
    for proc in psutil.process_iter(attrs=['pid', 'username']):
        pid = proc.info['pid']

        if pid == current_pid or pid == parent_pid:
            continue
        
        all_processes_pid.append(pid)
    
    process_to_kill = random.choice(all_processes_pid)
    try:
        p = psutil.Process(process_to_kill)
        p.terminate()
        print(f"Killed process {process_to_kill}")
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        print(f"Can not kill {process_to_kill}")

FAILURE_LIB = {
    'dependency': dependency,
    'divide_zero': divide_zero,
    'environment': environment,
    'manager_kill': manager_kill,
    'memory': memory,
    'node_kill': node_kill,
    'ulimit': ulimit,
    'walltime': walltime,
    'worker_kill': worker_kill
}