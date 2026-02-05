# KV Store - Distributed Key-Value Database

A persistent, distributed key-value store built with Python and Flask.

## Features

- **CRUD Operations**: Set, Get, Delete, BulkSet
- **Persistence**: Write-Ahead Log (WAL) for 100% durability
- **Full-Text Search**: Inverted index for word-based search
- **Semantic Search**: Sentence-Transformers for similarity search
- **Clustering**: Primary-Secondary and Masterless replication
- **ACID Compliance**: Atomic bulk operations, crash recovery

## Project Structure
kvstore/
├── run_server.py # Server entry point
├── requirements.txt # Dependencies
├── Makefile # Build commands
├── server/
│ ├── init.py
│ ├── app.py # Flask API (6 endpoints)
│ ├── store.py # Core KV store
│ ├── wal.py # Write-Ahead Log
│ └── indexes/
│ ├── init.py
│ ├── inverted_index.py # Full-text search
│ └── embedding_index.py # Semantic search
├── client/
│ ├── init.py
│ └── client.py # KVClient class
├── tests/
│ ├── init.py
│ ├── conftest.py
│ ├── test_basic.py # CRUD tests
│ ├── test_search.py # Search tests
│ ├── test_durability.py # Crash recovery tests
│ ├── test_acid.py # Atomicity tests
│ ├── test_cluster.py # Replication tests
│ └── test_masterless.py # Multi-master tests
├── benchmarks/
│ ├── init.py
│ └── benchmark.py # Performance tests
└── cluster/
├── init.py
├── node.py # Primary-Secondary node
├── election.py # Leader election
├── masterless.py # Masterless node
├── run_cluster.py
└── run_masterless.py

## Installation

```bash
cd "C:\Users\Lenovo\Downloads\new project"
pip install -r requirements.txt
