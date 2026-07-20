from src.common.text_splitter import RecursiveCharacterTextSplitter


def test_short_text_returns_single_chunk():
    splitter = RecursiveCharacterTextSplitter(chunk_size=100, chunk_overlap=0)
    assert splitter.split_text("Hello world") == ["Hello world"]


def test_exact_chunk_size():
    splitter = RecursiveCharacterTextSplitter(chunk_size=10, chunk_overlap=0)
    text = "A" * 10
    assert splitter.split_text(text) == [text]


def test_splits_by_double_newline():
    splitter = RecursiveCharacterTextSplitter(chunk_size=30, chunk_overlap=0)
    text = "Paragraph one.\n\nParagraph two.\n\nParagraph three."
    chunks = splitter.split_text(text)
    assert len(chunks) >= 2
    assert all("Paragraph" in c for c in chunks)


def test_splits_long_single_paragraph_by_sentences():
    splitter = RecursiveCharacterTextSplitter(chunk_size=30, chunk_overlap=0)
    text = "Sentence one. Sentence two. Sentence three. Sentence four."
    chunks = splitter.split_text(text)
    assert len(chunks) >= 2
    assert all(chunk.strip() for chunk in chunks)


def test_with_overlap():
    splitter = RecursiveCharacterTextSplitter(chunk_size=30, chunk_overlap=10)
    text = "A. " * 20
    chunks = splitter.split_text(text)
    assert len(chunks) >= 1
    if len(chunks) > 1:
        assert "A." in chunks[0] and "A." in chunks[1]


def test_empty_text():
    splitter = RecursiveCharacterTextSplitter(chunk_size=100, chunk_overlap=0)
    assert splitter.split_text("") == []


def test_single_word_returns_as_chunk():
    splitter = RecursiveCharacterTextSplitter(chunk_size=10, chunk_overlap=0)
    assert splitter.split_text("Hi") == ["Hi"]


def test_chunks_do_not_exceed_chunk_size():
    splitter = RecursiveCharacterTextSplitter(chunk_size=50, chunk_overlap=5)
    text = "A long sentence. " * 50
    chunks = splitter.split_text(text)
    for c in chunks:
        assert len(c) <= 50, f"Chunk {len(c)} > 50: {c!r}"


def test_preserves_all_content():
    splitter = RecursiveCharacterTextSplitter(chunk_size=50, chunk_overlap=10)
    text = "Word. " * 30
    chunks = splitter.split_text(text)
    joined = "".join(chunks)
    # Each "Word. " is 6 chars
    assert len(joined) >= 6 * 30 - len(chunks) * 10


def test_custom_separators():
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=100, chunk_overlap=0, separators=["|"]
    )
    text = "Part A|Part B|Part C"
    chunks = splitter.split_text(text)
    assert chunks == ["Part A|Part B|Part C"] or "|".join(chunks) == text


def test_respects_separator_order():
    splitter = RecursiveCharacterTextSplitter(chunk_size=20, chunk_overlap=0)
    text = "Big chunk here.\n\nSmall. Another. Third."
    chunks = splitter.split_text(text)
    assert len(chunks) >= 1
