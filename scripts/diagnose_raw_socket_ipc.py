"""Pure-Python reproduction of what Spark's PythonWorkerFactory does on
Windows for every single task: the parent process opens a TCP server
socket on 127.0.0.1, spawns a *child* python.exe process and tells it
the port via an environment variable, and waits for the child to connect
back and write a few bytes -- exactly the handshake that keeps failing
with EOFException in the real pipeline.

This has ZERO Spark, ZERO JVM, ZERO Py4J in it. If this script also
fails, the bug is proven to be at the Windows OS/security level (child
process spawn + loopback socket connect-back), not inside Spark,
PySpark, or our pipeline code -- and no amount of Spark config tuning
was ever going to fix it. If it succeeds, the problem is specific to
the JVM/Py4J/pyspark.worker protocol itself, not to Windows loopback
IPC in general.

Usage:
    python scripts/diagnose_raw_socket_ipc.py
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time

CHILD_MARKER = "__RAW_IPC_CHILD__"


def run_child() -> None:
    port = int(os.environ["RAW_IPC_PORT"])
    print(f"[child pid={os.getpid()}] connecting to 127.0.0.1:{port}...", file=sys.stderr, flush=True)
    t0 = time.monotonic()
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    print(f"[child] connected in {time.monotonic() - t0:.3f}s, sending payload", file=sys.stderr, flush=True)
    s.sendall(b"hello-from-child\n")
    s.close()
    print("[child] done", file=sys.stderr, flush=True)


def run_parent() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    print(f"[parent] listening on 127.0.0.1:{port}, spawning child python.exe...")

    env = dict(os.environ)
    env["RAW_IPC_PORT"] = str(port)
    proc = subprocess.Popen([sys.executable, __file__, CHILD_MARKER], env=env)

    srv.settimeout(10)
    try:
        conn, addr = srv.accept()
    except socket.timeout:
        print("FAILED: parent never received a connection within 10s (accept() timed out)")
        proc.kill()
        raise SystemExit(1)

    print(f"[parent] accepted connection from {addr}")
    conn.settimeout(10)
    try:
        data = conn.recv(4096)
    except socket.timeout:
        print("FAILED: connection accepted but child never sent data within 10s (this would mirror the EOFException)")
        raise SystemExit(1)
    finally:
        conn.close()

    proc.wait(timeout=10)

    if data == b"hello-from-child\n":
        print(f"SUCCESS: received {data!r} from child, child exit code {proc.returncode}")
    else:
        print(f"FAILED: unexpected/empty data received: {data!r}, child exit code {proc.returncode}")
        raise SystemExit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == CHILD_MARKER:
        run_child()
    else:
        run_parent()
