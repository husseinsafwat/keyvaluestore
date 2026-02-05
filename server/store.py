"""
Core Key-Value Store with WAL-based durability.
Supports debug parameter to simulate failures.
"""

import json
import os
import random
import threading
import time

from .wal import WAL
from .indexes.inverted_index import InvertedIndex
from .indexes.embedding_index import EmbeddingIndex


class KVStore:
    def __init__(self, data_dir="data", debug_failure_rate=0.01):
        self.data_dir = data_dir
        self.data_file = os.path.join(data_dir, "data.json")
        self.debug_failure_rate = debug_failure_rate
        
        # Core data structure
        self._data = {}
        self._lock = threading.RLock()
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize WAL
        self.wal = WAL(os.path.join(data_dir, "wal.log"))
        
        # Initialize indexes
        self.inverted_index = InvertedIndex(os.path.join(data_dir, "inverted_index.json"))
        self.embedding_index = EmbeddingIndex(os.path.join(data_dir, "embeddings"))
        
        # Recovery: load snapshot then replay WAL
        self._load_snapshot()
        self._replay_wal()
        
        # Background snapshot thread
        self._snapshot_interval = 30  # seconds
        self._stop_snapshot = threading.Event()
        self._snapshot_thread = threading.Thread(target=self._snapshot_loop, daemon=True)
        self._snapshot_thread.start()
    
    def set(self, key: str, value: str, debug: bool = False) -> dict:
        """
        Set a key-value pair.
        Returns {"success": bool, "seq": int}
        """
        with self._lock:
            # 1. Write to WAL first (synchronous)
            seq = self.wal.append("SET", key, value)
            
            # 2. Update memory
            old_value = self._data.get(key)
            self._data[key] = value
            
            # 3. Update indexes
            if old_value:
                self.inverted_index.update(key, old_value, value)
            else:
                self.inverted_index.add(key, value)
            self.embedding_index.update(key, value)
            
            # 4. Save to disk (may fail with debug mode)
            self._save(debug)
            
            return {"success": True, "seq": seq}
    
    def get(self, key: str) -> dict:
        """
        Get value by key.
        Returns {"success": bool, "value": str or None}
        """
        with self._lock:
            value = self._data.get(key)
            return {
                "success": value is not None,
                "value": value
            }
    
    def delete(self, key: str) -> dict:
        """
        Delete a key.
        Returns {"success": bool, "seq": int}
        """
        with self._lock:
            if key not in self._data:
                return {"success": False, "seq": None}
            
            # 1. Write to WAL
            seq = self.wal.append("DELETE", key)
            
            # 2. Get old value for index removal
            old_value = self._data.get(key)
            
            # 3. Remove from memory
            del self._data[key]
            
            # 4. Update indexes
            if old_value:
                self.inverted_index.remove(key, old_value)
                self.embedding_index.remove(key)
            
            # 5. Save to disk
            self._save(False)
            
            return {"success": True, "seq": seq}
    
    def bulk_set(self, items: list, debug: bool = False) -> dict:
        """
        Set multiple key-value pairs atomically.
        items: list of [key, value] or (key, value)
        Returns {"success": bool, "seq": int, "count": int}
        """
        with self._lock:
            # 1. Write to WAL atomically
            seq = self.wal.append_bulk(items)
            
            # 2. Apply all changes to memory
            for item in items:
                key, value = item[0], item[1]
                old_value = self._data.get(key)
                self._data[key] = value
                
                # Update indexes
                if old_value:
                    self.inverted_index.update(key, old_value, value)
                else:
                    self.inverted_index.add(key, value)
                self.embedding_index.update(key, value)
            
            # 3. Save to disk
            self._save(debug)
            
            return {"success": True, "seq": seq, "count": len(items)}
    
    def search_text(self, query: str, mode: str = "AND") -> list:
        """Full text search using inverted index."""
        return self.inverted_index.search(query, mode)
    
    def search_similar(self, query: str, top_k: int = 5) -> list:
        """Semantic search using embeddings."""
        return self.embedding_index.search(query, top_k)
    
    def _save(self, debug: bool = False):
        """
        Save data to disk.
        If debug=True, randomly skip save to simulate crash.
        """
        if debug:
            if random.random() < self.debug_failure_rate:
                return  # Simulate failure
        
        with open(self.data_file, 'w') as f:
            json.dump(self._data, f)
            f.flush()
            os.fsync(f.fileno())
    
    def _load_snapshot(self):
        """Load data from snapshot file."""
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r') as f:
                    self._data = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._data = {}
    
    def _replay_wal(self):
        """Replay WAL entries to recover uncommitted changes."""
        entries = self.wal.replay()
        
        for entry in entries:
            op = entry.get("op")
            
            if op == "SET":
                key = entry.get("key")
                value = entry.get("value")
                if key:
                    self._data[key] = value
                    self.inverted_index.add(key, value)
            
            elif op == "DELETE":
                key = entry.get("key")
                if key and key in self._data:
                    old_value = self._data.get(key)
                    del self._data[key]
                    if old_value:
                        self.inverted_index.remove(key, old_value)
            
            elif op == "BULK_SET":
                items = entry.get("items", [])
                for item in items:
                    key, value = item[0], item[1]
                    self._data[key] = value
                    self.inverted_index.add(key, value)
        
        # Save recovered state
        if entries:
            self._save(False)
            self.inverted_index.save()
    
    def _snapshot_loop(self):
        """Periodically save snapshot and clear WAL."""
        while not self._stop_snapshot.wait(self._snapshot_interval):
            self._create_snapshot()
    
    def _create_snapshot(self):
        """Create a snapshot and clear WAL."""
        with self._lock:
            self._save(False)
            self.inverted_index.save()
            self.embedding_index.save()
            self.wal.clear()
    
    def shutdown(self):
        """Graceful shutdown."""
        self._stop_snapshot.set()
        self._create_snapshot()
    
    def get_stats(self) -> dict:
        """Get store statistics."""
        with self._lock:
            return {
                "key_count": len(self._data),
                "wal_size": self.wal.get_size()
            }