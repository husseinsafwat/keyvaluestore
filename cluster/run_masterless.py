#!/usr/bin/env python3
"""
Script to run a masterless cluster node.
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cluster.masterless import MasterlessNode


def main():
    parser = argparse.ArgumentParser(description='Run a masterless node')
    parser.add_argument('--node-id', type=int, required=True, help='Node ID')
    parser.add_argument('--port', type=int, default=None, help='Port')
    
    args = parser.parse_args()
    
    node_id = args.node_id
    port = args.port or (6000 + node_id)
    
    all_nodes = [
        (1, "http://localhost:6001"),
        (2, "http://localhost:6002"),
        (3, "http://localhost:6003"),
    ]
    
    peers = [(nid, url) for nid, url in all_nodes if nid != node_id]
    
    print(f"Starting Masterless Node {node_id} on port {port}")
    
    node = MasterlessNode(
        node_id=node_id,
        port=port,
        peers=peers,
        data_dir=f"data_masterless_{node_id}"
    )
    
    try:
        node.start()
    except KeyboardInterrupt:
        node.shutdown()


if __name__ == '__main__':
    main()