                ,./ qZ# Fix: Remove `langchain_text_splitters` dependency from Flink chunker job

## Problem
`news_chunker_job.py` imports `langchain_text_splitters` which is not installed in the Flink Docker container. The Flink image only has `apache-flink` and its Python stdlib.

## Solution
Replace the `langchain_text_splitters.RecursiveCharacterTextSplitter` with an inline implementation. This is a straightforward recursive text splitter — no ML involved.

## Change: `src/streaming/news_chunker_job.py`

### Remove the import
```python
# Remove this line:
from langchain_text_splitters import RecursiveCharacterTextSplitter
```

### Add this inline class (before `chunk_article`)

```python
class RecursiveCharacterTextSplitter:
    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 64,
        separators: list[str] | None = None,
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.separators = separators or ["\n\n", "\n", ". ", " "]

    def split_text(self, text: str) -> list[str]:
        return self._split(text, self.separators)

    def _split(self, text: str, separators: list[str]) -> list[str]:
        final = []
        separator = separators[-1] if separators else ""

        if separators:
            separator = separators[0]
            splits = text.split(separator) if separator else list(text)
        else:
            splits = list(text)

        good_splits: list[str] = []
        for s in splits:
            if len(s) < self.chunk_size:
                good_splits.append(s)
            else:
                if good_splits:
                    final.extend(self._merge(good_splits, separator))
                    good_splits = []
                if len(separators) > 1:
                    final.extend(self._split(s, separators[1:]))
                else:
                    if separator:
                        for char in s:
                            good_splits.append(char)
                    else:
                        good_splits.append(s)

        if good_splits:
            final.extend(self._merge(good_splits, separator))

        return final

    def _merge(self, splits: list[str], separator: str) -> list[str]:
        docs: list[str] = []
        current: list[str] = []
        total = 0

        for d in splits:
            d_len = len(d)
            sep_len = len(separator) if current else 0

            if total + sep_len + d_len > self.chunk_size and current:
                doc = separator.join(current)
                if doc:
                    docs.append(doc)
                # Drop from front until we're under overlap
                while total > self.chunk_overlap and current:
                    removed = current.pop(0)
                    total -= len(removed)
                    if current:
                        total -= len(separator)

            current.append(d)
            total += d_len + (len(separator) if len(current) > 1 else 0)

        if current:
            doc = separator.join(current)
            if doc:
                docs.append(doc)

        return docs
```

### Module-level splitter stays the same
```python
_splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    separators=["\n\n", "\n", ". ", " "],
)
```

## Why this works
- `_split()` recursively tries separators in order — same strategy as langchain's version
- `_merge()` reassembles small pieces into chunks of up to `chunk_size`, dropping front items to respect `chunk_overlap`
- No external dependencies, pure Python, works in Flink's Python environment
