def simple_task():
    import time
    time.sleep(1)
    return 1

def fails():
    raise ValueError("Deliberate failure")

def depends(parent):
    return 1

def dependency_failure():
    f1 = fails()
    f2 = depends(f1)
    f3 = depends(f2)
    f4 = depends(f3)
    print(f1.result(), f2.result(), f3.result(), f4.result())

def open_many_files():
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