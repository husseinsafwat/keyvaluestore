#!/usr/bin/env python3
"""
Entry point for running the KV store server.
"""

import argparse
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server.app import create_app


def main():
    parser = argparse.ArgumentParser(description='KV Store Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind')
    parser.add_argument('--data-dir', default='data', help='Data directory')
    parser.add_argument('--debug', action='store_true', help='Debug mode')
    
    args = parser.parse_args()
    
    app = create_app(args.data_dir)
    print(f"Starting KV Store on {args.host}:{args.port}")
    print(f"Data directory: {args.data_dir}")
    
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == '__main__':
    main()