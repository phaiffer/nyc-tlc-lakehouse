"""Directly drives pyspark.worker's real entrypoint -- the exact module the
real Spark JVM spawns via `python -m pyspark.worker` on every single task
in local mode on Windows -- but plays the role of the JVM ourselves using
plain Python sockets implementing the real SocketAuthHelper wire protocol
(see pyspark/java_gateway.py:_do_server_auth and pyspark/serializers.py:
write_with_length/read_int/UTF8Deserializer, which we already read from
this venv's installed pyspark to make sure this is byte-for-byte correct).

Crucially: unlike Spark's Scala ProcessBuilder (which merges the worker's
stdout+stderr via redirectErrorStream(true), but which we've never seen
proof actually gets *read and printed* anywhere on the Windows path), we
capture the worker's stdout/stderr directly with subprocess.PIPE and print
everything live. If the real bug has been a Python-side crash/traceback
that Spark has simply never surfaced to us, this is where we'd finally see
it -- across 3 prior attempts (including with spark.python.worker.
faulthandler.enabled=true) we have seen exactly zero bytes of Python-side
output.

Protocol implemented, matching pyspark/worker.py's `if __name__ ==
"__main__"` block:
  1. worker connects to 127.0.0.1:<PYTHON_WORKER_FACTORY_PORT>
  2. worker writes a length-prefixed PYTHON_WORKER_FACTORY_SECRET (auth)
  3. we must reply with a length-prefixed "ok"
  4. worker writes its own pid as a 4-byte big-endian int
  5. worker then calls main(infile, outfile), which starts by blocking on
     read_int(infile) waiting for a task's split_index -- we have no real
     task to give it, so we stop right after step 4 succeeds, which is
     already past every point where the real pipeline has been crashing
     (that crash is an instant EOFException on the JVM's *first* read from
     the worker, i.e. it happens at or before step 4).

Usage:
    python scripts/diagnose_worker_direct.py
"""
from __future__ import annotations

import os
import secrets
import socket
import struct
import subprocess
import sys
import threading
import time


def read_int(stream) -> int:
    data = stream.read(4)
    if not data:
        raise EOFError("peer closed connection before sending a 4-byte int")
    return struct.unpack("!i", data)[0]


def write_with_length(data: bytes, stream) -> None:
    stream.write(struct.pack("!i", len(data)))
    stream.write(data)


def stream_pipe(pipe, label: str) -> None:
    for line in iter(pipe.readline, b""):
        print(f"[worker {label}] {line.decode(errors='replace').rstrip()}")


def main() -> None:
    secret = secrets.token_hex(16)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    print(f"[fake-jvm] listening on 127.0.0.1:{port}")

    env = dict(os.environ)
    env["PYTHON_WORKER_FACTORY_PORT"] = str(port)
    env["PYTHON_WORKER_FACTORY_SECRET"] = secret
    env["PYTHONUNBUFFERED"] = "YES"

    print(f"[fake-jvm] spawning: {sys.executable} -m pyspark.worker")
    proc = subprocess.Popen(
        [sys.executable, "-m", "pyspark.worker"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    threading.Thread(target=stream_pipe, args=(proc.stdout, "stdout"), daemon=True).start()
    threading.Thread(target=stream_pipe, args=(proc.stderr, "stderr"), daemon=True).start()

    srv.settimeout(15)
    try:
        conn, addr = srv.accept()
    except socket.timeout:
        print("FAILED: worker never connected back within 15s")
        proc.kill()
        raise SystemExit(1)

    print(f"[fake-jvm] accepted connection from {addr}")
    sockfile = conn.makefile("rwb", 65536)

    try:
        length = read_int(sockfile)
        received_secret = sockfile.read(length).decode("utf-8")
    except EOFError as exc:
        print(f"FAILED during auth read: {exc!r} -- worker crashed before writing anything at all")
        time.sleep(1)
        proc.wait(timeout=5)
        print(f"worker exit code: {proc.returncode}")
        raise SystemExit(1)

    print(f"[fake-jvm] received secret from worker (matches expected: {received_secret == secret})")

    write_with_length(b"ok", sockfile)
    sockfile.flush()
    print("[fake-jvm] sent 'ok' auth reply")

    try:
        worker_pid = read_int(sockfile)
    except EOFError as exc:
        print(f"FAILED reading worker pid after auth: {exc!r}")
        time.sleep(1)
        proc.wait(timeout=5)
        print(f"worker exit code: {proc.returncode}")
        raise SystemExit(1)

    print(f"SUCCESS: worker completed the auth handshake and reported its own pid = {worker_pid}")
    print("(this is already past every point where the real pipeline has been crashing)")

    proc.kill()
    time.sleep(0.5)


if __name__ == "__main__":
    main()
