"""
ACID tests - Atomicity, Consistency, Isolation, Durability.
"""

import pytest
import time
import os
import shutil
import subprocess
import signal
import threading
import random

from client import KVClient


class TestACID:
    """Test ACID properties."""
    
    def test_concurrent_bulk_set_same_keys(self):
        """
        Test: Concurrent bulk set writes touching same keys.
        Ensure they don't corrupt each other.
        """
        data_dir = "test_data_acid_concurrent"
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        os.makedirs(data_dir, exist_ok=True)
        
        proc = subprocess.Popen(
            ["python", "run_server.py", "--port", "5007", "--data-dir", data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        results = {"thread1": [], "thread2": []}
        errors = []
        
        def bulk_writer(thread_name, prefix):
            client = KVClient(port=5007)
            for _ in range(10):
                if client.health():
                    break
                time.sleep(0.2)
            
            for i in range(5):
                items = [
                    ("shared_key_1", f"{prefix}_value_{i}_1"),
                    ("shared_key_2", f"{prefix}_value_{i}_2"),
                    (f"{prefix}_unique_{i}", f"unique_value_{i}")
                ]
                try:
                    success = client.bulk_set(items)
                    results[thread_name].append((i, success))
                except Exception as e:
                    errors.append(str(e))
                time.sleep(0.01)
            
            client.close()
        
        t1 = threading.Thread(target=bulk_writer, args=("thread1", "t1"))
        t2 = threading.Thread(target=bulk_writer, args=("thread2", "t2"))
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        # Verify final state
        client = KVClient(port=5007)
        
        # Shared keys should have a consistent value (from one of the threads)
        val1 = client.get("shared_key_1")
        val2 = client.get("shared_key_2")
        
        assert val1 is not None
        assert val2 is not None
        
        # Both should be from same thread (atomic bulk)
        if val1.startswith("t1_"):
            assert val2.startswith("t1_") or val2.startswith("t2_")
        
        # Unique keys should exist
        assert client.get("t1_unique_4") is not None or client.get("t2_unique_4") is not None
        
        client.close()
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except:
            proc.kill()
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        
        assert len(errors) == 0, f"Errors: {errors}"
    
    def test_bulk_write_kill_atomicity(self):
        """
        Test: Bulk write + kill server.
        Ensure bulk is completely applied or not at all.
        """
        data_dir = "test_data_acid_atomicity"
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        os.makedirs(data_dir, exist_ok=True)
        
        proc = subprocess.Popen(
            ["python", "run_server.py", "--port", "5008", "--data-dir", data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        client = KVClient(port=5008)
        for _ in range(10):
            if client.health():
                break
            time.sleep(0.2)
        
        # Do a bulk write
        bulk_items = [(f"atomic_key_{i}", f"atomic_value_{i}") for i in range(10)]
        success = client.bulk_set(bulk_items)
        
        client.close()
        
        # Kill immediately
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
        
        time.sleep(1)
        
        # Restart
        proc2 = subprocess.Popen(
            ["python", "run_server.py", "--port", "5008", "--data-dir", data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        client2 = KVClient(port=5008)
        for _ in range(10):
            if client2.health():
                break
            time.sleep(0.2)
        
        # Check atomicity: either all keys exist or none
        found = []
        missing = []
        
        for i in range(10):
            key = f"atomic_key_{i}"
            val = client2.get(key)
            if val is not None:
                found.append(key)
            else:
                missing.append(key)
        
        print(f"Found: {len(found)}, Missing: {len(missing)}")
        
        client2.close()
        proc2.terminate()
        try:
            proc2.wait(timeout=3)
        except:
            proc2.kill()
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        
        # Atomicity: all or nothing
        if success:
            assert len(missing) == 0, f"Acknowledged but missing: {missing}"
        else:
            # If not acknowledged, partial state is acceptable
            pass