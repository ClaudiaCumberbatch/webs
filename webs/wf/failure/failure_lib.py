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

def memory():
    huge_memory_list = []
    while True:
        huge_memory_list.append('A' * 1024 * 1024 * 100)

def simple_task():
    import time
    time.sleep(1)
    return 1

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

FAILURE_LIB = {
    'dependency': dependency,
    'divide_zero': divide_zero,
    'environment': environment,
    'memory': memory,
    'simple': simple_task,
    'ulimit': ulimit,
    'walltime': walltime
}