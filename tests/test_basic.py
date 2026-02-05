"""
Basic CRUD tests for KV Store.
"""

import pytest
import time
import os
import shutil
import subprocess
import signal

from client import KVClient


class TestBasicOperations:
    """Test basic Set, Get, Delete operations."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test server."""
        self.data_dir = "test_data_basic"
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.proc = subprocess.Popen(
            ["python", "run_server.py", "--port", "5002", "--data-dir", self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        self.client = KVClient(port=5002)
        
        # Wait for ready
        for _ in range(10):
            if self.client.health():
                break
            time.sleep(0.3)
        
        yield
        
        self.client.close()
        self.proc.terminate()
        try:
            self.proc.wait(timeout=3)
        except:
            self.proc.kill()
        
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
    
    def test_set_then_get(self):
        """Test: Set then Get."""
        assert self.client.set("key1", "value1") == True
        assert self.client.get("key1") == "value1"
    
    def test_set_delete_get(self):
        """Test: Set then Delete then Get."""
        assert self.client.set("key2", "value2") == True
        assert self.client.get("key2") == "value2"
        assert self.client.delete("key2") == True
        assert self.client.get("key2") is None
    
    def test_get_without_setting(self):
        """Test: Get without setting."""
        assert self.client.get("nonexistent_key") is None
    
    def test_set_set_get(self):
        """Test: Set then Set (same key) then Get."""
        assert self.client.set("key3", "value3a") == True
        assert self.client.get("key3") == "value3a"
        assert self.client.set("key3", "value3b") == True
        assert self.client.get("key3") == "value3b"
    
    def test_bulk_set(self):
        """Test: Bulk set multiple keys."""
        items = [("bulk1", "val1"), ("bulk2", "val2"), ("bulk3", "val3")]
        assert self.client.bulk_set(items) == True
        
        assert self.client.get("bulk1") == "val1"
        assert self.client.get("bulk2") == "val2"
        assert self.client.get("bulk3") == "val3"


class TestPersistence:
    """Test data persistence across restarts."""
    
    def test_set_exit_get(self):
        """Test: Set then exit (gracefully) then Get."""
        data_dir = "test_data_persist"
        
        # Cleanup
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        os.makedirs(data_dir, exist_ok=True)
        
        # Start server
        proc = subprocess.Popen(
            ["python", "run_server.py", "--port", "5003", "--data-dir", data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        client = KVClient(port=5003)
        for _ in range(10):
            if client.health():
                break
            time.sleep(0.3)
        
        # Set data
        assert client.set("persist_key", "persist_value") == True
        assert client.get("persist_key") == "persist_value"
        
        # Gracefully stop
        proc.terminate()
        proc.wait(timeout=5)
        client.close()
        
        # Restart server
        proc2 = subprocess.Popen(
            ["python", "run_server.py", "--port", "5003", "--data-dir", data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        
        client2 = KVClient(port=5003)
        for _ in range(10):
            if client2.health():
                break
            time.sleep(0.3)
        
        # Verify data persisted
        assert client2.get("persist_key") == "persist_value"
        
        # Cleanup
        client2.close()
        proc2.terminate()
        try:
            proc2.wait(timeout=3)
        except:
            proc2.kill()
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)