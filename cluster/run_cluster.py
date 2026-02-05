#!/usr/bin/env python3
"""
Script to run a 3-node cluster.
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cluster.node import ClusterNode


def main():
    parser = argparse.ArgumentParser(description='Run a cluster node')
    parser.add_argument('--node-id', type=int, required=True, help='Node ID (1, 2, or 3)')
    parser.add_argument('--port', type=int, default=None, help='Port (default: 5000 + node_id)')
    
    args = parser.parse_args()
    
    node_id = args.node_id
    port = args.port or (5000 + node_id)
    
    # Define cluster topology
    all_nodes = [
        (1, "http://localhost:5001"),
        (2, "http://localhost:5002"),
        (3, "http://localhost:5003"),
    ]
    
    # Peers are all nodes except self
    peers = [(nid, url) for nid, url in all_nodes if nid != node_id]
    
    print(f"Starting Node {node_id} on port {port}")
    print(f"Peers: {peers}")
    
    node = ClusterNode(
        node_id=node_id,
        port=port,
        peers=peers,
        data_dir=f"data_node_{node_id}"
    )
    
    try:
        node.start()
    except KeyboardInterrupt:
        node.shutdown()


if __name__ == '__main__':
    main()