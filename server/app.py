"""
Flask server with 6 API endpoints.
"""

from flask import Flask, request, jsonify
import atexit

from .store import KVStore

app = Flask(__name__)
store = None


def init_store(data_dir="data"):
    """Initialize the KV store."""
    global store
    store = KVStore(data_dir=data_dir)
    return store


def get_store():
    """Get or create store instance."""
    global store
    if store is None:
        store = KVStore()
    return store


# Ensure graceful shutdown
@atexit.register
def shutdown():
    if store:
        store.shutdown()


# ============== API Endpoints ==============

@app.route('/set', methods=['POST'])
def set_key():
    """Set a key-value pair."""
    data = request.get_json()
    
    if not data or 'key' not in data or 'value' not in data:
        return jsonify({"success": False, "error": "Missing key or value"}), 400
    
    key = str(data['key'])
    value = str(data['value'])
    debug = data.get('debug', False)
    
    result = get_store().set(key, value, debug=debug)
    return jsonify(result)


@app.route('/get/<key>', methods=['GET'])
def get_key(key):
    """Get value by key."""
    result = get_store().get(key)
    
    if result['success']:
        return jsonify(result)
    else:
        return jsonify(result), 404


@app.route('/delete/<key>', methods=['DELETE'])
def delete_key(key):
    """Delete a key."""
    result = get_store().delete(key)
    
    if result['success']:
        return jsonify(result)
    else:
        return jsonify({"success": False, "error": "Key not found"}), 404


@app.route('/bulkset', methods=['POST'])
def bulk_set():
    """Bulk set multiple key-value pairs."""
    data = request.get_json()
    
    if not data or 'items' not in data:
        return jsonify({"success": False, "error": "Missing items"}), 400
    
    items = data['items']  # List of [key, value] pairs
    debug = data.get('debug', False)
    
    if not isinstance(items, list):
        return jsonify({"success": False, "error": "Items must be a list"}), 400
    
    result = get_store().bulk_set(items, debug=debug)
    return jsonify(result)


@app.route('/search/text', methods=['POST'])
def search_text():
    """Full text search."""
    data = request.get_json()
    
    if not data or 'query' not in data:
        return jsonify({"success": False, "error": "Missing query"}), 400
    
    query = data['query']
    mode = data.get('mode', 'AND')
    
    keys = get_store().search_text(query, mode)
    return jsonify({"success": True, "keys": keys})


@app.route('/search/similar', methods=['POST'])
def search_similar():
    """Semantic similarity search."""
    data = request.get_json()
    
    if not data or 'query' not in data:
        return jsonify({"success": False, "error": "Missing query"}), 400
    
    query = data['query']
    top_k = data.get('top_k', 5)
    
    results = get_store().search_similar(query, top_k)
    return jsonify({"success": True, "results": results})


@app.route('/stats', methods=['GET'])
def stats():
    """Get store statistics."""
    return jsonify(get_store().get_stats())


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok"})


def create_app(data_dir="data"):
    """Factory function to create app with custom data dir."""
    init_store(data_dir)
    return app


if __name__ == '__main__':
    init_store()
    app.run(host='0.0.0.0', port=5000, threaded=True)