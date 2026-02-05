"""
Masterless (Multi-Master) Replication.
All nodes can accept writes and replicate to others.
Uses vector clocks for conflict resolution (last-write-wins).
"""

import os
import threading
import time
import requests
from flask import Flask, request, jsonify

from server.store import KVStore


class MasterlessNode:
    def __init__(self, node_id: int, port: int, peers: list, data_dir: str = None):
        """
        Initialize a masterless node.
        
        Args:
            node_id: Unique node ID
            port: Port to listen on
            peers: List of (node_id, url) tuples
            data_dir: Data directory
        """
        self.node_id = node_id
        self.port = port
        self.peers = peers
        self.data_dir = data_dir or f"data_masterless_{node_id}"
        
        self.app = Flask(f"masterless_{node_id}")
        
        os.makedirs(self.data_dir, exist_ok=True)
        self.store = KVStore(data_dir=self.data_dir)
        
        # Vector clock: {key: {node_id: timestamp}}
        self.vector_clocks = {}
        self._clock_lock = threading.Lock()
        
        self._setup_routes()
    
    def _get_timestamp(self) -> float:
        """Get current timestamp."""
        return time.time()
    
    def _update_clock(self, key: str) -> dict:
        """Update vector clock for a key."""
        with self._clock_lock:
            if key not in self.vector_clocks:
                self.vector_clocks[key] = {}
            self.vector_clocks[key][self.node_id] = self._get_timestamp()
            return self.vector_clocks[key].copy()
    
    def _merge_clock(self, key: str, remote_clock: dict) -> bool:
        """
        Merge remote clock with local.
        Returns True if remote wins (should apply update).
        """
        with self._clock_lock:
            if key not in self.vector_clocks:
                self.vector_clocks[key] = {}
                return True
            
            local_clock = self.vector_clocks[key]
            
            # Last-write-wins based on max timestamp
            local_max = max(local_clock.values()) if local_clock else 0
            remote_max = max(remote_clock.values()) if remote_clock else 0
            
            if remote_max > local_max:
                # Remote wins, merge clocks
                for nid, ts in remote_clock.items():
                    if nid not in local_clock or ts > local_clock[nid]:
                        local_clock[nid] = ts
                return True
            
            return False
    
    def _setup_routes(self):
        """Setup Flask routes."""
        
        @self.app.route('/set', methods=['POST'])
        def set_key():
            data = request.get_json()
            key = str(data['key'])
            value = str(data['value'])
            debug = data.get('debug', False)
            
            # Update local
            result = self.store.set(key, value, debug=debug)
            clock = self._update_clock(key)
            
            # Replicate to peers
            if result['success']:
                self._replicate("SET", key, value, clock)
            
            return jsonify(result)
        
        @self.app.route('/get/<key>', methods=['GET'])
        def get_key(key):
            result = self.store.get(key)
            if result['success']:
                return jsonify(result)
            return jsonify(result), 404
        
        @self.app.route('/delete/<key>', methods=['DELETE'])
        def delete_key(key):
            result = self.store.delete(key)
            clock = self._update_clock(key)
            
            if result['success']:
                self._replicate("DELETE", key, None, clock)
                return jsonify(result)
            return jsonify({"success": False, "error": "Key not found"}), 404
        
        @self.app.route('/bulkset', methods=['POST'])
        def bulk_set():
            data = request.get_json()
            items = data['items']
            debug = data.get('debug', False)
            
            result = self.store.bulk_set(items, debug=debug)
            
            if result['success']:
                clocks = {}
                for key, value in items:
                    clocks[key] = self._update_clock(key)
                self._replicate("BULK_SET", None, None, clocks, items=items)
            
            return jsonify(result)
        
        @self.app.route('/replicate', methods=['POST'])
        def replicate():
            data = request.get_json()
            op = data['op']
            clock = data.get('clock', {})
            
            if op == "SET":
                key = data['key']
                if self._merge_clock(key, clock):
                    self.store.set(key, data['value'])
            
            elif op == "DELETE":
                key = data['key']
                if self._merge_clock(key, clock):
                    self.store.delete(key)
            
            elif op == "BULK_SET":
                items = data['items']
                clocks = data.get('clocks', {})
                for key, value in items:
                    if self._merge_clock(key, clocks.get(key, {})):
                        self.store.set(key, value)
            
            return jsonify({"success": True})
        
        @self.app.route('/health', methods=['GET'])
        def health():
            return jsonify({"status": "ok", "node_id": self.node_id})
        
        @self.app.route('/stats', methods=['GET'])
        def stats():
            store_stats = self.store.get_stats()
            store_stats['node_id'] = self.node_id
            return jsonify(store_stats)
    
    def _replicate(self, op: str, key: str, value: str, clock: dict, items: list = None):
        """Replicate to all peers asynchronously."""
        payload = {"op": op, "key": key, "value": value, "clock": clock}
        if items:
            payload["items"] = items
            payload["clocks"] = clock  # clock is dict of clocks for bulk
        
        def send_to_peer(url):
            try:
                requests.post(f"{url}/replicate", json=payload, timeout=5)
            except requests.RequestException:
                pass
        
        for nid, url in self.peers:
            threading.Thread(target=send_to_peer, args=(url,), daemon=True).start()
    
    def start(self):
        """Start the node."""
        self.app.run(host='0.0.0.0', port=self.port, threaded=True)
    
    def shutdown(self):
        """Shutdown the node."""
        self.store.shutdown()