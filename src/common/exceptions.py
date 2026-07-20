class RAGError(Exception):
    pass


class EmbeddingError(RAGError):
    pass


class VectorStoreError(RAGError):
    pass


class LLMError(RAGError):
    pass
