"""
Cluster Node with Primary-Secondary Replication.
"""

import os
import threading
import time
import requests
from flask import Flask, request, jsonify

from server.store import KVStore
from .election import LeaderElection


class ClusterNode:
    def __init__(self, node_id: int, port: int, peers: list, data_dir: str = None):
        """
        Initialize a cluster node.
        
        Args:
            node_id: Unique node ID
            port: Port to listen on
            peers: List of (node_id, url) tuples for other nodes
            data_dir: Data directory
        """
        self.node_id = node_id
        self.port = port
        self.peers = peers
        self.data_dir = data_dir or f"data_node_{node_id}"
        
        # Create Flask app
        self.app = Flask(f"node_{node_id}")
        
        # Initialize store
        os.makedirs(self.data_dir, exist_ok=True)
        self.store = KVStore(data_dir=self.data_dir)
        
        # Initialize election
        self.election = LeaderElection(
            node_id=node_id,
            peers=peers,
            on_become_leader=self._on_become_leader
        )
        
        # Setup routes
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup Flask routes."""
        
        @self.app.route('/set', methods=['POST'])
        def set_key():
            # Only primary handles writes
            if not self.election.is_leader:
                leader_url = self.election.get_leader_url()
                if leader_url:
                    # Forward to leader
                    try:
                        resp = requests.post(f"{leader_url}/set", json=request.get_json(), timeout=10)
                        return jsonify(resp.json()), resp.status_code
                    except requests.RequestException as e:
                        return jsonify({"success": False, "error": "Leader unavailable"}), 503
                return jsonify({"success": False, "error": "No leader"}), 503
            
            data = request.get_json()
            key = str(data['key'])
            value = str(data['value'])
            debug = data.get('debug', False)
            
            result = self.store.set(key, value, debug=debug)
            
            # Replicate to secondaries
            if result['success']:
                self._replicate_to_secondaries("SET", key, value)
            
            return jsonify(result)
        
        @self.app.route('/get/<key>', methods=['GET'])
        def get_key(key):
            # Reads can happen on any node (for simplicity, forward to leader)
            if not self.election.is_leader:
                leader_url = self.election.get_leader_url()
                if leader_url:
                    try:
                        resp = requests.get(f"{leader_url}/get/{key}", timeout=10)
                        return jsonify(resp.json()), resp.status_code
                    except requests.RequestException:
                        return jsonify({"success": False, "error": "Leader unavailable"}), 503
                return jsonify({"success": False, "error": "No leader"}), 503
            
            result = self.store.get(key)
            if result['success']:
                return jsonify(result)
            return jsonify(result), 404
        
        @self.app.route('/delete/<key>', methods=['DELETE'])
        def delete_key(key):
            if not self.election.is_leader:
                leader_url = self.election.get_leader_url()
                if leader_url:
                    try:
                        resp = requests.delete(f"{leader_url}/delete/{key}", timeout=10)
                        return jsonify(resp.json()), resp.status_code
                    except requests.RequestException:
                        return jsonify({"success": False, "error": "Leader unavailable"}), 503
                return jsonify({"success": False, "error": "No leader"}), 503
            
            result = self.store.delete(key)
            
            if result['success']:
                self._replicate_to_secondaries("DELETE", key, None)
                return jsonify(result)
            return jsonify({"success": False, "error": "Key not found"}), 404
        
        @self.app.route('/bulkset', methods=['POST'])
        def bulk_set():
            if not self.election.is_leader:
                leader_url = self.election.get_leader_url()
                if leader_url:
                    try:
                        resp = requests.post(f"{leader_url}/bulkset", json=request.get_json(), timeout=30)
                        return jsonify(resp.json()), resp.status_code
                    except requests.RequestException:
                        return jsonify({"success": False, "error": "Leader unavailable"}), 503
                return jsonify({"success": False, "error": "No leader"}), 503
            
            data = request.get_json()
            items = data['items']
            debug = data.get('debug', False)
            
            result = self.store.bulk_set(items, debug=debug)
            
            if result['success']:
                self._replicate_to_secondaries("BULK_SET", None, None, items=items)
            
            return jsonify(result)
        
        @self.app.route('/search/text', methods=['POST'])
        def search_text():
            data = request.get_json()
            query = data['query']
            mode = data.get('mode', 'AND')
            keys = self.store.search_text(query, mode)
            return jsonify({"success": True, "keys": keys})
        
        @self.app.route('/search/similar', methods=['POST'])
        def search_similar():
            data = request.get_json()
            query = data['query']
            top_k = data.get('top_k', 5)
            results = self.store.search_similar(query, top_k)
            return jsonify({"success": True, "results": results})
        
        # Replication endpoint (called by primary)
        @self.app.route('/replicate', methods=['POST'])
        def replicate():
            data = request.get_json()
            op = data['op']
            
            if op == "SET":
                self.store.set(data['key'], data['value'])
            elif op == "DELETE":
                self.store.delete(data['key'])
            elif op == "BULK_SET":
                self.store.bulk_set(data['items'])
            
            return jsonify({"success": True})
        
        # Election endpoints
        @self.app.route('/election', methods=['POST'])
        def election():
            data = request.get_json()
            self.election.receive_election(data['from'])
            return jsonify({"success": True})
        
        @self.app.route('/coordinator', methods=['POST'])
        def coordinator():
            data = request.get_json()
            self.election.receive_coordinator(data['leader_id'])
            return jsonify({"success": True})
        
        @self.app.route('/heartbeat', methods=['POST'])
        def heartbeat():
            data = request.get_json()
            self.election.receive_heartbeat(data['leader_id'])
            return jsonify({"success": True})
        
        @self.app.route('/health', methods=['GET'])
        def health():
            return jsonify({
                "status": "ok",
                "node_id": self.node_id,
                "is_leader": self.election.is_leader,
                "leader_id": self.election.leader_id
            })
        
        @self.app.route('/stats', methods=['GET'])
        def stats():
            store_stats = self.store.get_stats()
            store_stats['node_id'] = self.node_id
            store_stats['is_leader'] = self.election.is_leader
            return jsonify(store_stats)
    
    def _replicate_to_secondaries(self, op: str, key: str, value: str, items: list = None):
        """Replicate operation to secondary nodes."""
        payload = {"op": op, "key": key, "value": value}
        if items:
            payload["items"] = items
        
        for nid, url in self.peers:
            try:
                requests.post(f"{url}/replicate", json=payload, timeout=5)
            except requests.RequestException:
                print(f"[Node {self.node_id}] Failed to replicate to {nid}")
    
    def _on_become_leader(self):
        """Callback when this node becomes leader."""
        print(f"[Node {self.node_id}] Now acting as primary")
    
    def start(self):
        """Start the node."""
        # Start election
        self.election.start()
        
        # Start Flask
        self.app.run(host='0.0.0.0', port=self.port, threaded=True)
    
    def shutdown(self):
        """Shutdown the node."""
        self.election.stop()
        self.store.shutdown()