"""
Durability tests - testing data survival after crashes.
"""

import pytest
import time
import os
import shutil
import subprocess
import signal
import threading

from client import KVClient


class TestDurability:
    """Test durability with random kills."""
    
    def test_acknowledged_data_survives_kill(self):
        """
        Test: Add data, track acknowledgments, kill server, check survival.
        """
        data_dir = "test_data_durability"
        
        # Cleanup
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        os.makedirs(data_dir, exist_ok=True)
        
        # Start server
        proc = subprocess.Popen(
            ["python", "run_server.py", "--port", "5005", "--data-dir", data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        client = KVClient(port=5005)
        for _ in range(10):
            if client.health():
                break
            time.sleep(0.3)
        
        # Write data and track what was acknowledged
        acknowledged_keys = []
        for i in range(20):
            key = f"durability_key_{i}"
            value = f"durability_value_{i}"
            if client.set(key, value):
                acknowledged_keys.append(key)
        
        client.close()
        
        # SIGKILL the server (-9)
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
        
        time.sleep(1)
        
        # Restart server
        proc2 = subprocess.Popen(
            ["python", "run_server.py", "--port", "5005", "--data-dir", data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        client2 = KVClient(port=5005)
        for _ in range(10):
            if client2.health():
                break
            time.sleep(0.3)
        
        # Check acknowledged data survived
        lost_keys = []
        for key in acknowledged_keys:
            if client2.get(key) is None:
                lost_keys.append(key)
        
        print(f"Acknowledged: {len(acknowledged_keys)}, Lost: {len(lost_keys)}")
        
        # Cleanup
        client2.close()
        proc2.terminate()
        try:
            proc2.wait(timeout=3)
        except:
            proc2.kill()
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        
        # 100% durability - no acknowledged data should be lost
        assert len(lost_keys) == 0, f"Lost keys: {lost_keys}"
    
    def test_concurrent_write_and_kill(self):
        """
        Test: One thread writes, another thread kills randomly.
        """
        data_dir = "test_data_concurrent_kill"
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        os.makedirs(data_dir, exist_ok=True)
        
        acknowledged = []
        stop_writing = threading.Event()
        
        def writer_thread():
            client = KVClient(port=5006)
            i = 0
            while not stop_writing.is_set():
                key = f"concurrent_key_{i}"
                value = f"concurrent_value_{i}"
                try:
                    if client.set(key, value):
                        acknowledged.append(key)
                except:
                    pass
                i += 1
                time.sleep(0.05)
            client.close()
        
        # Start server
        proc = subprocess.Popen(
            ["python", "run_server.py", "--port", "5006", "--data-dir", data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        # Start writer
        writer = threading.Thread(target=writer_thread)
        writer.start()
        
        # Let it write for a bit
        time.sleep(1)
        
        # Kill server
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
        
        stop_writing.set()
        writer.join()
        
        time.sleep(1)
        
        # Restart server
        proc2 = subprocess.Popen(
            ["python", "run_server.py", "--port", "5006", "--data-dir", data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        client = KVClient(port=5006)
        for _ in range(10):
            if client.health():
                break
            time.sleep(0.3)
        
        # Check acknowledged data
        lost = []
        for key in acknowledged:
            if client.get(key) is None:
                lost.append(key)
        
        print(f"Acknowledged: {len(acknowledged)}, Lost: {len(lost)}")
        
        # Cleanup
        client.close()
        proc2.terminate()
        try:
            proc2.wait(timeout=3)
        except:
            proc2.kill()
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        
        assert len(lost) == 0, f"Lost {len(lost)} keys"