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
        final: list[str] = []
        if not separators:
            return self._merge(list(text), "")
        separator = separators[0]
        splits = text.split(separator) if separator else list(text)
        good_splits: list[str] = []
        for s in splits:
            if not s:
                continue
            if len(s) < self.chunk_size:
                good_splits.append(s)
            else:
                if good_splits:
                    final.extend(self._merge(good_splits, separator))
                    good_splits = []
                final.extend(self._split(s, separators[1:]))
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
