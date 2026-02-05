"""
Write-Ahead Log (WAL) for 100% durability.
Every operation is logged BEFORE being applied.
On crash recovery, replay WAL to restore state.
"""

import json
import os
import time
import threading
import fcntl


class WAL:
    def __init__(self, wal_path="data/wal.log"):
        self.wal_path = wal_path
        self._lock = threading.Lock()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(wal_path), exist_ok=True)
        
        # Create WAL file if not exists
        if not os.path.exists(wal_path):
            open(wal_path, 'w').close()
    
    def append(self, operation: str, key: str, value=None) -> int:
        """
        Append operation to WAL synchronously.
        Returns sequence number for acknowledgment tracking.
        """
        entry = {
            "seq": int(time.time() * 1000000),  # Microsecond timestamp as seq
            "op": operation,  # "SET", "DELETE", "BULK_SET"
            "key": key,
            "value": value,
            "ts": time.time()
        }
        
        with self._lock:
            with open(self.wal_path, 'a') as f:
                # Get exclusive lock for file
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        
        return entry["seq"]
    
    def append_bulk(self, items: list) -> int:
        """
        Append bulk operation atomically to WAL.
        All items are written as a single entry.
        """
        entry = {
            "seq": int(time.time() * 1000000),
            "op": "BULK_SET",
            "items": items,  # List of [key, value] pairs
            "ts": time.time()
        }
        
        with self._lock:
            with open(self.wal_path, 'a') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        
        return entry["seq"]
    
    def replay(self) -> list:
        """
        Read all entries from WAL for recovery.
        Returns list of operations to replay.
        """
        entries = []
        
        if not os.path.exists(self.wal_path):
            return entries
        
        with self._lock:
            with open(self.wal_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            entry = json.loads(line)
                            entries.append(entry)
                        except json.JSONDecodeError:
                            continue  # Skip corrupted entries
        
        return entries
    
    def clear(self):
        """Clear WAL after successful snapshot."""
        with self._lock:
            with open(self.wal_path, 'w') as f:
                f.flush()
                os.fsync(f.fileno())
    
    def get_size(self) -> int:
        """Get WAL file size in bytes."""
        if os.path.exists(self.wal_path):
            return os.path.getsize(self.wal_path)
        return 0