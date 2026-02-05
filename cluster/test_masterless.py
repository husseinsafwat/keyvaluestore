"""
Tests for masterless replication.
"""

import pytest
import time
import os
import shutil
import subprocess
import signal

from client import KVClient


class TestMasterless:
    """Test masterless replication."""
    
    def _start_node(self, node_id):
        """Start a masterless node."""
        data_dir = f"test_masterless_data_{node_id}"
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        
        port = 6010 + node_id
        proc = subprocess.Popen(
            ["python", "-c", f"""
import sys
sys.path.insert(0, '.')
from cluster.masterless import MasterlessNode

peers = [
    (1, "http://localhost:6011"),
    (2, "http://localhost:6012"),
    (3, "http://localhost:6013"),
]
peers = [(nid, url) for nid, url in peers if nid != {node_id}]

node = MasterlessNode(
    node_id={node_id},
    port={port},
    peers=peers,
    data_dir="{data_dir}"
)
node.start()
"""],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return proc, port, data_dir
    
    def _cleanup_node(self, proc, data_dir):
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except:
            proc.kill()
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
    
    def test_write_to_any_node(self):
        """Test that writes can go to any node."""
        procs = []
        data_dirs = []
        
        try:
            for node_id in [1, 2, 3]:
                proc, port, data_dir = self._start_node(node_id)
                procs.append(proc)
                data_dirs.append(data_dir)
            
            time.sleep(3)
            
            # Write to node 1
            client1 = KVClient(port=6011)
            for _ in range(10):
                if client1.health():
                    break
                time.sleep(0.3)
            
            assert client1.set("key1", "value1") == True
            
            time.sleep(1)  # Wait for replication
            
            # Read from node 2
            client2 = KVClient(port=6012)
            for _ in range(10):
                if client2.health():
                    break
                time.sleep(0.3)
            
            assert client2.get("key1") == "value1"
            
            # Write to node 2
            assert client2.set("key2", "value2") == True
            
            time.sleep(1)
            
            # Read from node 3
            client3 = KVClient(port=6013)
            for _ in range(10):
                if client3.health():
                    break
                time.sleep(0.3)
            
            assert client3.get("key2") == "value2"
            
            client1.close()
            client2.close()
            client3.close()
        
        finally:
            for proc, data_dir in zip(procs, data_dirs):
                self._cleanup_node(proc, data_dir)
    
    def test_concurrent_writes_same_key(self):
        """Test concurrent writes to same key (last-write-wins)."""
        procs = []
        data_dirs = []
        
        try:
            for node_id in [1, 2]:
                proc, port, data_dir = self._start_node(node_id)
                procs.append(proc)
                data_dirs.append(data_dir)
            
            time.sleep(3)
            
            client1 = KVClient(port=6011)
            client2 = KVClient(port=6012)
            
            for _ in range(10):
                if client1.health() and client2.health():
                    break
                time.sleep(0.3)
            
            # Write same key from both nodes
            client1.set("conflict_key", "value_from_1")
            time.sleep(0.1)
            client2.set("conflict_key", "value_from_2")
            
            time.sleep(2)  # Wait for replication
            
            # Both should have same value (last write wins)
            val1 = client1.get("conflict_key")
            val2 = client2.get("conflict_key")
            
            assert val1 == val2  # Consistent
            
            client1.close()
            client2.close()
        
        finally:
            for proc, data_dir in zip(procs, data_dirs):
                self._cleanup_node(proc, data_dir)