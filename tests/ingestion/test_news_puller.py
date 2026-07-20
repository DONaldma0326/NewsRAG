import hashlib
import time
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.news_puller import (
    init_state_store,
    load_seen_ids,
    load_source_headers,
    normalize_entry,
    process_source,
    prune_seen_ids,
    save_seen_ids,
    save_source_headers,
)

# ─── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def state_db(tmp_path):
    db = tmp_path / "test_state.db"
    conn = init_state_store(db)
    yield conn
    conn.close()


@pytest.fixture
def mock_producer():
    p = MagicMock()
    p.flush.return_value = None
    return p


# ─── State store tests ─────────────────────────────────────────────────


class TestInitStateStore:
    def test_creates_tables(self, state_db):
        tables = state_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        assert [row[0] for row in tables] == ["seen_articles", "source_state"]


class TestSourceHeaders:
    def test_round_trip(self, state_db):
        save_source_headers(state_db, "test-source", "abc123", 1234567890)
        headers = load_source_headers(state_db)
        assert headers == {"test-source": {"etag": "abc123", "modified_ts": 1234567890}}

    def test_update_existing(self, state_db):
        save_source_headers(state_db, "src", "etag1", 100)
        save_source_headers(state_db, "src", "etag2", 200)
        headers = load_source_headers(state_db)
        assert headers["src"]["etag"] == "etag2"

    def test_null_values(self, state_db):
        save_source_headers(state_db, "src", None, None)
        headers = load_source_headers(state_db)
        assert headers["src"] == {"etag": None, "modified_ts": None}

    def test_empty_when_no_sources(self, state_db):
        assert load_source_headers(state_db) == {}


class TestSeenArticles:
    def test_save_and_load(self, state_db):
        save_seen_ids(state_db, {"id1", "id2"})
        assert load_seen_ids(state_db) == {"id1", "id2"}

    def test_append(self, state_db):
        save_seen_ids(state_db, {"id1"})
        save_seen_ids(state_db, {"id2"})
        assert load_seen_ids(state_db) == {"id1", "id2"}

    def test_ignore_duplicates(self, state_db):
        save_seen_ids(state_db, {"id1"})
        save_seen_ids(state_db, {"id1"})
        assert load_seen_ids(state_db) == {"id1"}

    def test_empty(self, state_db):
        assert load_seen_ids(state_db) == set()


class TestPruneSeenIds:
    def test_removes_old_entries(self, state_db):
        save_seen_ids(state_db, {"fresh_id"})
        state_db.execute(
            "INSERT INTO seen_articles (id, seen_at) VALUES (?, ?)",
            ("stale_id", "2000-01-01T00:00:00+00:00"),
        )
        state_db.commit()
        prune_seen_ids(state_db)
        assert load_seen_ids(state_db) == {"fresh_id"}

    def test_keeps_recent_entries(self, state_db):
        save_seen_ids(state_db, {"recent"})
        prune_seen_ids(state_db)
        assert "recent" in load_seen_ids(state_db)


# ─── Domain logic tests ────────────────────────────────────────────────


class TestNormalizeEntry:
    def test_creates_expected_schema(self):
        entry = {
            "title": "Test",
            "link": "http://example.com",
            "published": "now",
            "summary": "sum",
        }
        result = normalize_entry("bbc", entry)
        expected_id = hashlib.sha256(b"http://example.com").hexdigest()
        assert result == {
            "source": "bbc",
            "title": "Test",
            "link": "http://example.com",
            "published": "now",
            "summary": "sum",
            "id": expected_id,
        }

    def test_same_link_same_id(self):
        e1 = {"link": "http://example.com/a"}
        e2 = {"link": "http://example.com/a"}
        assert normalize_entry("src", e1)["id"] == normalize_entry("src", e2)["id"]

    def test_different_link_different_id(self):
        e1 = {"link": "http://example.com/a"}
        e2 = {"link": "http://example.com/b"}
        assert normalize_entry("src", e1)["id"] != normalize_entry("src", e2)["id"]

    def test_defaults_when_fields_missing(self):
        result = normalize_entry("src", {})
        assert result["title"] == ""
        assert result["link"] == ""
        assert result["summary"] == ""


# ─── process_source tests (mock feedparser) ────────────────────────────


def make_fake_feed(entries, status=200, etag=None, modified=None, bozo=False):
    feed = MagicMock()
    feed.status = status
    feed.entries = entries
    feed.etag = etag
    feed.modified = modified
    feed.bozo = bozo
    feed.get = lambda key, default=None: getattr(feed, key, default)
    return feed


def make_fake_entry(link: str) -> dict:
    return {"title": f"Article {link}", "link": link, "published": None, "summary": ""}


class TestProcessSource:
    def test_skips_304(self, state_db, mock_producer):
        """Feed returns 304 → no articles produced, nothing saved."""
        source = {"name": "test", "url": "http://feed"}
        seen: set = set()
        source_headers: dict = {}

        with patch(
            "src.ingestion.news_puller.feedparser.parse",
            return_value=make_fake_feed([], status=304),
        ):
            count = process_source(
                mock_producer, source, seen, source_headers, state_db
            )

        assert count == 0
        assert seen == set()
        mock_producer.produce.assert_not_called()
        mock_producer.flush.assert_not_called()

    def test_produces_new_articles(self, state_db, mock_producer):
        """New articles are produced and tracked."""
        source = {"name": "test", "url": "http://feed"}
        seen: set = set()
        source_headers: dict = {}
        entries = [make_fake_entry("http://a"), make_fake_entry("http://b")]
        fake_feed = make_fake_feed(entries, etag="new-etag", modified=time.gmtime(1000))

        with patch(
            "src.ingestion.news_puller.feedparser.parse", return_value=fake_feed
        ):
            count = process_source(
                mock_producer, source, seen, source_headers, state_db
            )

        assert count == 2
        assert len(seen) == 2
        assert source_headers["test"]["etag"] == "new-etag"
        assert source_headers["test"]["modified_ts"] == 1000
        assert mock_producer.produce.call_count == 2
        mock_producer.flush.assert_called_once()

    def test_skips_seen_articles(self, state_db, mock_producer):
        """Articles already in seen set are not produced."""
        source = {"name": "test", "url": "http://feed"}
        a_id = hashlib.sha256(b"http://a").hexdigest()
        b_id = hashlib.sha256(b"http://b").hexdigest()
        seen = {a_id}
        source_headers: dict = {}
        entries = [make_fake_entry("http://a"), make_fake_entry("http://b")]
        fake_feed = make_fake_feed(entries)

        with patch(
            "src.ingestion.news_puller.feedparser.parse", return_value=fake_feed
        ):
            count = process_source(
                mock_producer, source, seen, source_headers, state_db
            )

        assert count == 1
        assert a_id in seen
        assert b_id in seen
        mock_producer.produce.assert_called_once()

    def test_persists_source_headers(self, state_db, mock_producer):
        """Source etag/modified are saved to DB after processing."""
        source = {"name": "test", "url": "http://feed"}
        entries = [make_fake_entry("http://a")]
        fake_feed = make_fake_feed(entries, etag="my-etag", modified=time.gmtime(500))

        with patch(
            "src.ingestion.news_puller.feedparser.parse", return_value=fake_feed
        ):
            process_source(mock_producer, source, set(), {}, state_db)

        headers = load_source_headers(state_db)
        assert headers["test"]["etag"] == "my-etag"
        assert headers["test"]["modified_ts"] == 500

    def test_persists_seen_ids(self, state_db, mock_producer):
        """New article IDs are saved to DB after processing."""
        source = {"name": "test", "url": "http://feed"}
        entries = [make_fake_entry("http://a")]
        fake_feed = make_fake_feed(entries)

        with patch(
            "src.ingestion.news_puller.feedparser.parse", return_value=fake_feed
        ):
            process_source(mock_producer, source, set(), {}, state_db)

        seen = load_seen_ids(state_db)
        assert len(seen) == 1

    def test_uses_etag_from_headers(self, state_db, mock_producer):
        """Calls feedparser with stored etag."""
        source = {"name": "test", "url": "http://feed"}
        source_headers = {"test": {"etag": "stored-etag", "modified_ts": None}}
        fake_feed = make_fake_feed([make_fake_entry("http://a")], status=304)

        with patch(
            "src.ingestion.news_puller.feedparser.parse", return_value=fake_feed
        ) as mock_parse:
            process_source(mock_producer, source, set(), source_headers, state_db)

        mock_parse.assert_called_once_with(
            "http://feed", etag="stored-etag", modified=None
        )

    def test_uses_modified_from_headers(self, state_db, mock_producer):
        """Calls feedparser with stored modified timestamp."""
        source = {"name": "test", "url": "http://feed"}
        source_headers = {"test": {"etag": None, "modified_ts": 12345}}
        fake_feed = make_fake_feed([make_fake_entry("http://a")], status=304)

        with patch(
            "src.ingestion.news_puller.feedparser.parse", return_value=fake_feed
        ) as mock_parse:
            process_source(mock_producer, source, set(), source_headers, state_db)

        kwargs = mock_parse.call_args.kwargs
        assert kwargs["etag"] is None
        expected_modified = time.gmtime(12345)
        assert kwargs["modified"] == expected_modified

    def test_handles_malformed_feed(self, state_db, mock_producer):
        """Malformed feed with no entries is skipped."""
        source = {"name": "test", "url": "http://feed"}
        feed = make_fake_feed([], bozo=True)
        feed.status = 200

        with patch("src.ingestion.news_puller.feedparser.parse", return_value=feed):
            count = process_source(mock_producer, source, set(), {}, state_db)

        assert count == 0
        mock_producer.produce.assert_not_called()
