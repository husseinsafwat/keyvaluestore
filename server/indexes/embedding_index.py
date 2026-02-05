"""
Embedding Index for semantic search using Sentence-Transformers.
Converts values to vectors and finds similar ones via cosine similarity.
"""

import os
import json
import threading
import numpy as np

# Lazy load sentence-transformers to speed up startup
_model = None
_model_lock = threading.Lock()


def get_model():
    """Lazy load the sentence transformer model."""
    global _model
    if _model is None:
        with _model_lock:
            if _model is None:
                from sentence_transformers import SentenceTransformer
                _model = SentenceTransformer('all-MiniLM-L6-v2')
    return _model


class EmbeddingIndex:
    def __init__(self, index_path="data/embeddings"):
        self.index_path = index_path
        self.embeddings = {}  # key -> numpy array
        self._lock = threading.Lock()
        
        os.makedirs(index_path, exist_ok=True)
        self._load()
    
    def add(self, key: str, value: str):
        """Generate and store embedding for a value."""
        if not isinstance(value, str) or not value.strip():
            return
        
        model = get_model()
        vector = model.encode(value, convert_to_numpy=True)
        
        with self._lock:
            self.embeddings[key] = vector
    
    def remove(self, key: str):
        """Remove embedding for a key."""
        with self._lock:
            if key in self.embeddings:
                del self.embeddings[key]
    
    def update(self, key: str, value: str):
        """Update embedding for a key."""
        self.add(key, value)
    
    def search(self, query: str, top_k: int = 5) -> list:
        """
        Find top-k most similar keys to query.
        Returns list of (key, score) tuples.
        """
        if not query.strip():
            return []
        
        model = get_model()
        query_vec = model.encode(query, convert_to_numpy=True)
        
        with self._lock:
            if not self.embeddings:
                return []
            
            scores = {}
            for key, vec in self.embeddings.items():
                # Cosine similarity
                similarity = np.dot(query_vec, vec) / (
                    np.linalg.norm(query_vec) * np.linalg.norm(vec)
                )
                scores[key] = float(similarity)
            
            # Sort by similarity descending
            sorted_keys = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            return sorted_keys[:top_k]
    
    def save(self):
        """Persist embeddings to disk."""
        with self._lock:
            # Save embeddings as numpy file
            embeddings_file = os.path.join(self.index_path, "vectors.npz")
            keys_file = os.path.join(self.index_path, "keys.json")
            
            if self.embeddings:
                keys = list(self.embeddings.keys())
                vectors = np.array([self.embeddings[k] for k in keys])
                np.savez_compressed(embeddings_file, vectors=vectors)
                with open(keys_file, 'w') as f:
                    json.dump(keys, f)
    
    def _load(self):
        """Load embeddings from disk."""
        embeddings_file = os.path.join(self.index_path, "vectors.npz")
        keys_file = os.path.join(self.index_path, "keys.json")
        
        if os.path.exists(embeddings_file) and os.path.exists(keys_file):
            try:
                with open(keys_file, 'r') as f:
                    keys = json.load(f)
                data = np.load(embeddings_file)
                vectors = data['vectors']
                
                self.embeddings = {k: v for k, v in zip(keys, vectors)}
            except Exception:
                self.embeddings = {}
    
    def clear(self):
        """Clear the index."""
        with self._lock:
            self.embeddings = {}