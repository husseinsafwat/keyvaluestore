"""
KV Store Client class.
"""

import requests
from typing import Optional, List, Tuple


class KVClient:
    def __init__(self, host: str = "localhost", port: int = 5000):
        self.base_url = f"http://{host}:{port}"
        self.session = requests.Session()
    
    def set(self, key: str, value: str, debug: bool = False) -> bool:
        """
        Set a key-value pair.
        Returns True if successful.
        """
        try:
            response = self.session.post(
                f"{self.base_url}/set",
                json={"key": key, "value": value, "debug": debug},
                timeout=10
            )
            return response.status_code == 200 and response.json().get("success", False)
        except requests.RequestException:
            return False
    
    def get(self, key: str) -> Optional[str]:
        """
        Get value by key.
        Returns value if found, None otherwise.
        """
        try:
            response = self.session.get(
                f"{self.base_url}/get/{key}",
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    return data.get("value")
            return None
        except requests.RequestException:
            return None
    
    def delete(self, key: str) -> bool:
        """
        Delete a key.
        Returns True if successful.
        """
        try:
            response = self.session.delete(
                f"{self.base_url}/delete/{key}",
                timeout=10
            )
            return response.status_code == 200 and response.json().get("success", False)
        except requests.RequestException:
            return False
    
    def bulk_set(self, items: List[Tuple[str, str]], debug: bool = False) -> bool:
        """
        Set multiple key-value pairs atomically.
        items: List of (key, value) tuples.
        Returns True if successful.
        """
        try:
            # Convert tuples to lists for JSON
            items_list = [[k, v] for k, v in items]
            response = self.session.post(
                f"{self.base_url}/bulkset",
                json={"items": items_list, "debug": debug},
                timeout=30
            )
            return response.status_code == 200 and response.json().get("success", False)
        except requests.RequestException:
            return False
    
    def search_text(self, query: str, mode: str = "AND") -> List[str]:
        """
        Full text search on values.
        Returns list of keys matching the query.
        """
        try:
            response = self.session.post(
                f"{self.base_url}/search/text",
                json={"query": query, "mode": mode},
                timeout=10
            )
            if response.status_code == 200:
                return response.json().get("keys", [])
            return []
        except requests.RequestException:
            return []
    
    def search_similar(self, query: str, top_k: int = 5) -> List[Tuple[str, float]]:
        """
        Semantic similarity search.
        Returns list of (key, score) tuples.
        """
        try:
            response = self.session.post(
                f"{self.base_url}/search/similar",
                json={"query": query, "top_k": top_k},
                timeout=30
            )
            if response.status_code == 200:
                results = response.json().get("results", [])
                return [(r[0], r[1]) for r in results]
            return []
        except requests.RequestException:
            return []
    
    def stats(self) -> dict:
        """Get store statistics."""
        try:
            response = self.session.get(f"{self.base_url}/stats", timeout=10)
            if response.status_code == 200:
                return response.json()
            return {}
        except requests.RequestException:
            return {}
    
    def health(self) -> bool:
        """Check if server is healthy."""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            return False
    
    def close(self):
        """Close the session."""
        self.session.close()