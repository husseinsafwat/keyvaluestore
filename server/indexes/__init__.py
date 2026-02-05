# server/indexes/__init__.py
from .inverted_index import InvertedIndex
from .embedding_index import EmbeddingIndex

__all__ = ['InvertedIndex', 'EmbeddingIndex']