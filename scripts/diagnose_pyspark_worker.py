"""Minimal, fast repro for the 'Python worker exited unexpectedly (crashed)'
EOFException that run-all hits during the Silver stage.

This does NOT touch Bronze/Silver/the 2.9M-row dataset at all -- it starts a
local Spark session with exactly one core, tries to materialize a single
literal row via spark.createDataFrame(), and prints whatever the JVM and
worker report. If this script fails the same way, the bug is 100%
isolated to "can a Python worker on this machine ever complete a callback
to the JVM at all" -- nothing about Delta, the metastore, or dataset size.

Usage:
    python scripts/diagnose_pyspark_worker.py

Read the whole output, especially:
  - Any line starting "Worker Faulthandler" or containing a Python
    traceback -- that would be the first real Python-side error we've
    seen in this whole investigation.
  - "Windows fatal exception" -- a native crash (DLL/antivirus interference).
  - Whether it prints "SUCCESS" at the end or dies before that.
"""
from __future__ import annotations

import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession  # noqa: E402

print(f"Using interpreter: {sys.executable}")
print(f"PYSPARK_PYTHON={os.environ.get('PYSPARK_PYTHON')}")
print(f"PYSPARK_DRIVER_PYTHON={os.environ.get('PYSPARK_DRIVER_PYTHON')}")

spark = (
    SparkSession.builder.appName("worker-smoke-test")
    .master("local[1]")  # single core: only ever one worker process, ever
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
    # Ask the worker to install Python's faulthandler and dump a traceback
    # to stderr if it segfaults/dies unexpectedly, instead of just vanishing.
    .config("spark.python.worker.faulthandler.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("INFO")

print("SparkSession created. Attempting spark.createDataFrame(...).collect() ...")
try:
    df = spark.createDataFrame([(1, "hello")], ["id", "text"])
    rows = df.collect()
    print(f"SUCCESS: collected rows = {rows}")
except Exception as exc:  # noqa: BLE001
    print(f"FAILED with exception: {type(exc).__name__}: {exc}")
    raise
finally:
    spark.stop()
