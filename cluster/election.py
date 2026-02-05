"""
Leader Election using simple Bully Algorithm.
"""

import time
import threading
import requests
from typing import Callable, Optional


class LeaderElection:
    def __init__(self, node_id: int, peers: list, on_become_leader: Callable = None):
        """
        Initialize leader election.
        
        Args:
            node_id: Unique ID for this node (higher ID = higher priority)
            peers: List of peer URLs [(id, url), ...]
            on_become_leader: Callback when this node becomes leader
        """
        self.node_id = node_id
        self.peers = peers  # [(id, url), ...]
        self.on_become_leader = on_become_leader
        
        self.leader_id = None
        self.is_leader = False
        self._lock = threading.Lock()
        self._election_in_progress = False
        self._heartbeat_interval = 2  # seconds
        self._heartbeat_timeout = 5  # seconds
        self._last_heartbeat = time.time()
        
        self._stop_event = threading.Event()
        self._monitor_thread = None
    
    def start(self):
        """Start the election monitor."""
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        
        # Start initial election
        self.start_election()
    
    def stop(self):
        """Stop the election monitor."""
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=3)
    
    def _monitor_loop(self):
        """Monitor leader health and trigger election if needed."""
        while not self._stop_event.is_set():
            time.sleep(1)
            
            if self.is_leader:
                # Send heartbeats to followers
                self._send_heartbeats()
            else:
                # Check if leader is alive
                if time.time() - self._last_heartbeat > self._heartbeat_timeout:
                    print(f"[Node {self.node_id}] Leader timeout, starting election")
                    self.start_election()
    
    def start_election(self):
        """Start a new election."""
        with self._lock:
            if self._election_in_progress:
                return
            self._election_in_progress = True
        
        print(f"[Node {self.node_id}] Starting election")
        
        # Find nodes with higher IDs
        higher_nodes = [(nid, url) for nid, url in self.peers if nid > self.node_id]
        
        if not higher_nodes:
            # I have the highest ID, I'm the leader
            self._become_leader()
            return
        
        # Send election messages to higher nodes
        got_response = False
        for nid, url in higher_nodes:
            try:
                resp = requests.post(f"{url}/election", json={"from": self.node_id}, timeout=2)
                if resp.status_code == 200:
                    got_response = True
                    break
            except requests.RequestException:
                continue
        
        if not got_response:
            # No higher node responded, I'm the leader
            self._become_leader()
        else:
            # Wait for coordinator message
            with self._lock:
                self._election_in_progress = False
    
    def _become_leader(self):
        """This node becomes the leader."""
        with self._lock:
            self.is_leader = True
            self.leader_id = self.node_id
            self._election_in_progress = False
        
        print(f"[Node {self.node_id}] I am now the leader")
        
        # Announce to all peers
        for nid, url in self.peers:
            try:
                requests.post(f"{url}/coordinator", json={"leader_id": self.node_id}, timeout=2)
            except requests.RequestException:
                continue
        
        if self.on_become_leader:
            self.on_become_leader()
    
    def receive_election(self, from_id: int) -> bool:
        """
        Receive an election message from a lower node.
        Returns True to indicate we're alive.
        """
        print(f"[Node {self.node_id}] Received election from {from_id}")
        
        # Start our own election if not already in progress
        if not self._election_in_progress:
            threading.Thread(target=self.start_election, daemon=True).start()
        
        return True
    
    def receive_coordinator(self, leader_id: int):
        """Receive coordinator announcement."""
        with self._lock:
            self.leader_id = leader_id
            self.is_leader = (leader_id == self.node_id)
            self._election_in_progress = False
            self._last_heartbeat = time.time()
        
        print(f"[Node {self.node_id}] New leader is {leader_id}")
    
    def receive_heartbeat(self, from_leader: int):
        """Receive heartbeat from leader."""
        with self._lock:
            if from_leader == self.leader_id:
                self._last_heartbeat = time.time()
    
    def _send_heartbeats(self):
        """Send heartbeats to all followers."""
        for nid, url in self.peers:
            try:
                requests.post(f"{url}/heartbeat", json={"leader_id": self.node_id}, timeout=1)
            except requests.RequestException:
                continue
    
    def get_leader_url(self) -> Optional[str]:
        """Get the current leader's URL."""
        if self.is_leader:
            return None  # We are the leader
        
        for nid, url in self.peers:
            if nid == self.leader_id:
                return url
        return None