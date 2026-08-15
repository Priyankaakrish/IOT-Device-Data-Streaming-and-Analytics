#!/usr/bin/env python3
"""
pytest configuration for the Spark tests.

These environment variables MUST be set before the first SparkSession is
created, which is why they live here rather than in a fixture -- conftest.py is
imported before any test module.

The problem this solves
-----------------------
On a Windows host running Docker Desktop, the test suite failed with:

    org.apache.spark.SparkException: Python worker failed to connect back
    Caused by: java.net.SocketTimeoutException: Accept timed out
    ... (host.docker.internal executor driver)

Docker Desktop writes `host.docker.internal` into the Windows hosts file.
Java's InetAddress.getLocalHost() resolves to that name, so Spark binds the
driver to it and then tells the Python worker to call back on that address.
The worker cannot reach it and the accept() times out after 60 seconds.

Pinning the driver to the loopback address removes the ambiguity. The streaming
job does not hit this because it is launched through spark-submit with an
explicit driver host.
"""

from __future__ import annotations

import os
import sys

# Bind Spark's driver to loopback rather than whatever getLocalHost() decides.
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Point Spark at the exact interpreter running pytest. Without this, Spark
# falls back to whatever `python` resolves to on PATH -- often a different
# version, or nothing at all on a machine that only has the `py` launcher.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Inherited from the streaming job's shell, PYSPARK_SUBMIT_ARGS makes the test
# session try to resolve the Kafka and Postgres jars from Maven. The tests need
# neither, and on a machine without network access the resolution hangs.
os.environ.pop("PYSPARK_SUBMIT_ARGS", None)

# Java 17 closed off the reflective access Spark's serialisers rely on.
# Harmless on Java 11.
os.environ.setdefault(
    "JAVA_TOOL_OPTIONS",
    " ".join(
        [
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        ]
    ),
)

# The tests import streaming_job_star directly, so spark/ must be importable
# whether pytest is invoked from the repo root or from inside spark/.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
