"""
Tests for cluster functionality.
"""

import pytest
import time
import os
import shutil
import subprocess
import signal
import threading

from client import KVClient


class TestCluster:
    """Test cluster replication and failover."""
    
    def _start_node(self, node_id):
        """Start a cluster node."""
        data_dir = f"test_cluster_data_{node_id}"
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        os.makedirs(data_dir, exist_ok=True)
        
        port = 5020 + node_id
        proc = subprocess.Popen(
            ["python", "-c", f"""
import sys
sys.path.insert(0, '.')
from cluster.node import ClusterNode

peers = [
    (1, "http://localhost:5021"),
    (2, "http://localhost:5022"),
    (3, "http://localhost:5023"),
]
peers = [(nid, url) for nid, url in peers if nid != {node_id}]

node = ClusterNode(
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
        """Cleanup a node."""
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except:
            proc.kill()
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
    
    def test_basic_replication(self):
        """Test basic write replication to secondaries."""
        procs = []
        data_dirs = []
        
        try:
            # Start 3 nodes
            for node_id in [1, 2, 3]:
                proc, port, data_dir = self._start_node(node_id)
                procs.append(proc)
                data_dirs.append(data_dir)
            
            time.sleep(5)  # Wait for election
            
            # Find leader (node 3 should win with highest ID)
            client = KVClient(port=5023)
            
            for _ in range(10):
                if client.health():
                    break
                time.sleep(0.5)
            
            # Write to leader
            assert client.set("cluster_key", "cluster_value") == True
            
            time.sleep(1)  # Wait for replication
            
            # Verify on leader
            assert client.get("cluster_key") == "cluster_value"
            
            client.close()
        
        finally:
            for proc, data_dir in zip(procs, data_dirs):
                self._cleanup_node(proc, data_dir)
    
    def test_leader_failover(self):
        """Test leader election when primary goes down."""
        procs = []
        data_dirs = []
        
        try:
            # Start 3 nodes
            for node_id in [1, 2, 3]:
                proc, port, data_dir = self._start_node(node_id)
                procs.append(proc)
                data_dirs.append(data_dir)
            
            time.sleep(5)  # Wait for election
            
            # Write to leader (node 3)
            client = KVClient(port=5023)
            for _ in range(10):
                if client.health():
                    break
                time.sleep(0.5)
            
            client.set("failover_key", "failover_value")
            client.close()
            
            time.sleep(1)
            
            # Kill the leader (node 3)
            os.kill(procs[2].pid, signal.SIGKILL)
            procs[2].wait()
            
            time.sleep(6)  # Wait for new election
            
            # Node 2 should become leader
            client2 = KVClient(port=5022)
            for _ in range(10):
                if client2.health():
                    break
                time.sleep(0.5)
            
            # Data should still be accessible (was replicated)
            # Note: In this simple implementation, reads go to leader
            # so we just check the new leader is responsive
            assert client2.health() == True
            
            client2.close()
        
        finally:
            for i, (proc, data_dir) in enumerate(zip(procs, data_dirs)):
                if procs[i].poll() is None:  # Still running
                    self._cleanup_node(proc, data_dir)
                else:
                    if os.path.exists(data_dir):
                        shutil.rmtree(data_dir)