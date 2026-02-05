"""
Pytest fixtures for KV Store tests.
"""

import pytest
import subprocess
import time
import os
import shutil
import signal


@pytest.fixture(scope="function")
def server_process():
    """Start a fresh server for each test."""
    # Clean data directory
    data_dir = "test_data"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    
    # Start server
    proc = subprocess.Popen(
        ["python", "run_server.py", "--port", "5001", "--data-dir", data_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for server to start
    time.sleep(2)
    
    yield proc
    
    # Cleanup
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except:
        proc.kill()
    
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)


@pytest.fixture(scope="function")
def client(server_process):
    """Create a client connected to the test server."""
    from client import KVClient
    c = KVClient(port=5001)
    
    # Wait for server to be ready
    for _ in range(10):
        if c.health():
            break
        time.sleep(0.5)
    
    yield c
    c.close()


def start_server(port=5001, data_dir="test_data"):
    """Helper to start a server process."""
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    
    proc = subprocess.Popen(
        ["python", "run_server.py", "--port", str(port), "--data-dir", data_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(2)
    return proc


def kill_server(proc):
    """Kill server with SIGKILL (-9)."""
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait()


def stop_server(proc):
    """Gracefully stop server."""
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except:
        proc.kill()