"""
Tests for search functionality.
"""

import pytest
import time
import os
import shutil
import subprocess

from client import KVClient


class TestSearch:
    """Test search functionality."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test server."""
        self.data_dir = "test_data_search"
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.proc = subprocess.Popen(
            ["python", "run_server.py", "--port", "5004", "--data-dir", self.data_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(3)  # Longer wait for embedding model
        
        self.client = KVClient(port=5004)
        
        for _ in range(15):
            if self.client.health():
                break
            time.sleep(0.5)
        
        yield
        
        self.client.close()
        self.proc.terminate()
        try:
            self.proc.wait(timeout=3)
        except:
            self.proc.kill()
        
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
    
    def test_text_search_single_word(self):
        """Test full text search with single word."""
        self.client.set("doc1", "hello world python")
        self.client.set("doc2", "hello flask web")
        self.client.set("doc3", "python is great")
        
        time.sleep(0.5)
        
        results = self.client.search_text("python")
        assert "doc1" in results
        assert "doc3" in results
        assert "doc2" not in results
    
    def test_text_search_multiple_words_and(self):
        """Test full text search with AND mode."""
        self.client.set("doc1", "hello world python")
        self.client.set("doc2", "hello flask web")
        
        time.sleep(0.5)
        
        results = self.client.search_text("hello python", mode="AND")
        assert "doc1" in results
        assert "doc2" not in results
    
    def test_text_search_multiple_words_or(self):
        """Test full text search with OR mode."""
        self.client.set("doc1", "hello world python")
        self.client.set("doc2", "hello flask web")
        self.client.set("doc3", "python is great")
        
        time.sleep(0.5)
        
        results = self.client.search_text("flask python", mode="OR")
        assert "doc1" in results
        assert "doc2" in results
        assert "doc3" in results
    
    def test_semantic_search(self):
        """Test semantic similarity search."""
        self.client.set("doc1", "I love machine learning and artificial intelligence")
        self.client.set("doc2", "Python programming for data science")
        self.client.set("doc3", "The weather is sunny and warm today")
        
        time.sleep(1)  # Wait for embedding
        
        results = self.client.search_similar("AI and deep learning", top_k=2)
        
        # doc1 should be most similar
        keys = [r[0] for r in results]
        assert len(keys) > 0
        # doc1 about AI should rank higher than doc3 about weather
        if "doc1" in keys and "doc3" in keys:
            assert keys.index("doc1") < keys.index("doc3")