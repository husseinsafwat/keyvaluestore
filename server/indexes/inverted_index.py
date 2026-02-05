"""
Inverted Index for full-text search on values.
Maps words -> list of keys containing that word.
"""

import re
import json
import os
import threading


class InvertedIndex:
    def __init__(self, index_path="data/inverted_index.json"):
        self.index_path = index_path
        self.index = {}  # word -> set of keys
        self._lock = threading.Lock()
        
        os.makedirs(os.path.dirname(index_path), exist_ok=True)
        self._load()
    
    def _tokenize(self, text: str) -> list:
        """Split text into lowercase words."""
        if not isinstance(text, str):
            text = str(text)
        # Remove punctuation and split
        words = re.findall(r'\b\w+\b', text.lower())
        return words
    
    def add(self, key: str, value: str):
        """Index a key-value pair."""
        words = self._tokenize(value)
        
        with self._lock:
            for word in words:
                if word not in self.index:
                    self.index[word] = set()
                self.index[word].add(key)
    
    def remove(self, key: str, value: str = None):
        """Remove a key from index."""
        with self._lock:
            for word in list(self.index.keys()):
                if key in self.index[word]:
                    self.index[word].discard(key)
                    if not self.index[word]:
                        del self.index[word]
    
    def update(self, key: str, old_value: str, new_value: str):
        """Update index when value changes."""
        self.remove(key, old_value)
        self.add(key, new_value)
    
    def search(self, query: str, mode="AND") -> list:
        """
        Search for keys containing query words.
        mode: "AND" (all words) or "OR" (any word)
        """
        words = self._tokenize(query)
        
        if not words:
            return []
        
        with self._lock:
            results = []
            for word in words:
                if word in self.index:
                    results.append(self.index[word].copy())
                else:
                    results.append(set())
            
            if mode == "AND":
                # Intersection - keys must contain ALL words
                if results:
                    final = results[0]
                    for r in results[1:]:
                        final = final.intersection(r)
                    return list(final)
            else:
                # Union - keys containing ANY word
                final = set()
                for r in results:
                    final = final.union(r)
                return list(final)
        
        return []
    
    def save(self):
        """Persist index to disk."""
        with self._lock:
            # Convert sets to lists for JSON
            serializable = {k: list(v) for k, v in self.index.items()}
            with open(self.index_path, 'w') as f:
                json.dump(serializable, f)
    
    def _load(self):
        """Load index from disk."""
        if os.path.exists(self.index_path):
            try:
                with open(self.index_path, 'r') as f:
                    data = json.load(f)
                    self.index = {k: set(v) for k, v in data.items()}
            except (json.JSONDecodeError, IOError):
                self.index = {}
    
    def clear(self):
        """Clear the index."""
        with self._lock:
            self.index = {}