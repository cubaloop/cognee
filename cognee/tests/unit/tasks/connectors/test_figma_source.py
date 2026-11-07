import pytest

from cognee.tasks.ingestion.connectors import build_figma_service, figma_source
from cognee.tasks.ingestion.connectors.figma import (
    FigmaAuthError,
    FigmaClient,
    FigmaRateLimitedError,
    _iter_rows,
)
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    NODE_SET_COLUMN,
    PIPELINE_SCOPE_ATTR,
)


class FakeFigmaClient:
    def __init__(self, files_by_key=None, comments_by_key=None):
        self.files_by_key = files_by_key or {}
        self.comments_by_key = comments_by_key or {}
        self.calls = []

    def get_file(self, file_key: str):
        self.calls.append(("get_file", file_key))
        val = self.files_by_key[file_key]
        if isinstance(val, Exception):
            raise val
        return val

    def get_comments(self, file_key: str):
        self.calls.append(("get_comments", file_key))
        val = self.comments_by_key.get(file_key, [])
        if isinstance(val, Exception):
            raise val
        return val


def _sample_figma_file(version="101", last_modified="2026-11-01T10:00:00Z", include_second_page=True):
    pages = [
        {
            "id": "0:1",
            "name": "Checkout Flow",
            "type": "CANVAS",
            "children": [
                {
                    "id": "1:10",
                    "name": "Hero Card",
                    "type": "COMPONENT",
                    "description": "Primary hero checkout card",
                    "transitionNodeID": "2:10",
                    "transitionEasing": "EASE_IN_AND_OUT",
                    "children": [
                        {
                            "id": "1:11",
                            "name": "Headline",
                            "type": "TEXT",
                            "characters": "Complete your purchase",
                        }
                    ],
                }
            ],
        }
    ]
    if include_second_page:
        pages.append(
            {
                "id": "0:2",
                "name": "Design Tokens",
                "type": "CANVAS",
                "children": [
                    {
                        "id": "2:10",
                        "name": "Token Label",
                        "type": "TEXT",
                        "characters": "Primary Brass #7A5A1E",
                    }
                ],
            }
        )
    return {
        "name": "Acme Design System",
        "version": version,
        "lastModified": last_modified,
        "schemaVersion": 0,
        "editorType": "figma",
        "document": {
            "id": "0:0",
            "name": "Document",
            "type": "DOCUMENT",
            "children": pages,
        },
        "components": {
            "1:10": {"name": "Hero Card", "description": "Primary hero checkout card"},
        },
        "componentSets": {
            "9:1": {"name": "Button Variants", "description": "Primary and secondary states"},
        },
        "styles": {
            "S:1": {"name": "Brand/Brass", "styleType": "FILL", "description": "Warm brass accent"},
        },
    }


def _sample_comments():
    return [
        {
            "id": "c1",
            "message": "Align button padding to 16px",
            "created_at": "2026-11-01T11:00:00Z",
            "user": {"handle": "alice"},
            "client_meta": {"node_id": "1:11"},
        },
        {
            "id": "c2",
            "parent_id": "c1",
            "message": "Updated in latest component revision",
            "created_at": "2026-11-01T11:05:00Z",
            "user": {"handle": "bob"},
        },
        {
            "id": "c3",
            "message": "File-level review approved",
            "created_at": "2026-11-01T12:00:00Z",
            "resolved_at": "2026-11-01T12:30:00Z",
            "user": {"handle": "carol"},
        },
    ]


def test_figma_source_extracts_file_pages_components_text_and_comments():
    fake = FakeFigmaClient(
        files_by_key={"FileKey123": _sample_figma_file()},
        comments_by_key={"FileKey123": _sample_comments()},
    )
    state = {}
    stats = {"synced": 0, "deleted": 0, "failed": 0, "failed_rate_limit": 0, "failed_budget": 0, "failed_auth": 0}

    rows = list(_iter_rows(fake, ["FileKey123"], state, stats))
    assert len(rows) == 3
    assert stats["synced"] == 3
    assert stats["deleted"] == 0

    file_row = rows[0]
    assert file_row["id"] == "figma:file:FileKey123"
    assert file_row["_deleted"] is False
    assert file_row[NODE_SET_COLUMN] == ["figma:FileKey123"]
    assert "Acme Design System" in file_row["content"]
    assert "Hero Card (id=1:10): Primary hero checkout card" in file_row["content"]
    assert "Button Variants (id=9:1): Primary and secondary states" in file_row["content"]
    assert "Brand/Brass [FILL]: Warm brass accent" in file_row["content"]
    assert "- carol [resolved]: File-level review approved" in file_row["content"]

    page1 = rows[1]
    assert page1["id"] == "figma:page:FileKey123:0:1"
    assert "Hero Card > Headline: Complete your purchase" in page1["content"]
    assert "[COMPONENT] Hero Card (id=1:10): Primary hero checkout card" in page1["content"]
    assert "Hero Card (1:10) -> target node 2:10 [EASE_IN_AND_OUT]" in page1["content"]
    assert "- alice (node 1:11): Align button padding to 16px" in page1["content"]
    assert "  - bob: Updated in latest component revision" in page1["content"]

    page2 = rows[2]
    assert page2["id"] == "figma:page:FileKey123:0:2"
    assert "Token Label: Primary Brass #7A5A1E" in page2["content"]


def test_figma_incremental_skips_unchanged_and_tombstones_removed_pages_and_files():
    fake = FakeFigmaClient(
        files_by_key={"FileKey123": _sample_figma_file()},
        comments_by_key={"FileKey123": _sample_comments()},
    )
    state = {}
    stats = {"synced": 0, "deleted": 0, "failed": 0, "failed_rate_limit": 0, "failed_budget": 0, "failed_auth": 0}

    first_rows = list(_iter_rows(fake, ["FileKey123"], state, stats))
    assert len(first_rows) == 3

    # Second run with identical version/lastModified/comments yields 0 rows
    stats2 = {"synced": 0, "deleted": 0, "failed": 0, "failed_rate_limit": 0, "failed_budget": 0, "failed_auth": 0}
    second_rows = list(_iter_rows(fake, ["FileKey123"], state, stats2))
    assert second_rows == []
    assert stats2["synced"] == 0

    # Third run where page 0:2 was deleted upstream in a new file version
    fake.files_by_key["FileKey123"] = _sample_figma_file(
        version="102", last_modified="2026-11-02T10:00:00Z", include_second_page=False
    )
    stats3 = {"synced": 0, "deleted": 0, "failed": 0, "failed_rate_limit": 0, "failed_budget": 0, "failed_auth": 0}
    third_rows = list(_iter_rows(fake, ["FileKey123"], state, stats3))
    assert stats3["synced"] == 2
    assert stats3["deleted"] == 1
    tombstones = [r for r in third_rows if r["_deleted"]]
    assert len(tombstones) == 1
    assert tombstones[0]["id"] == "figma:page:FileKey123:0:2"


def test_figma_rate_limit_stops_cleanly_and_records_stats():
    fake = FakeFigmaClient(
        files_by_key={"FileKey123": FigmaRateLimitedError(retry_after=30)},
    )
    state = {}
    stats = {"synced": 0, "deleted": 0, "failed": 0, "failed_rate_limit": 0, "failed_budget": 0, "failed_auth": 0}

    rows = list(_iter_rows(fake, ["FileKey123"], state, stats))
    assert rows == []
    assert stats["failed"] == 1
    assert stats["failed_rate_limit"] == 1


def test_figma_client_headers_and_source_metadata():
    pat_client = build_figma_service("figd_secret_token_123")
    assert pat_client._headers()["X-Figma-Token"] == "figd_secret_token_123"
    assert "secret" not in repr(pat_client)

    oauth_client = FigmaClient("figu_oauth_token_456")
    assert oauth_client._headers()["Authorization"] == "Bearer figu_oauth_token_456"

    res = figma_source(file_keys=["FileKey123"], service=FakeFigmaClient())
    assert getattr(res, DOCUMENT_SOURCE_ATTR) == "figma"
    assert getattr(res, PIPELINE_SCOPE_ATTR).startswith("figma_")

    with pytest.raises(ValueError, match="file_keys"):
        figma_source(file_keys=[], access_token="figd_123")

    with pytest.raises(FigmaAuthError):
        bad_fake = FakeFigmaClient(files_by_key={"FileKey123": FigmaAuthError("403")})
        stats = {"synced": 0, "deleted": 0, "failed": 0, "failed_rate_limit": 0, "failed_budget": 0, "failed_auth": 0}
        list(_iter_rows(bad_fake, ["FileKey123"], {}, stats))
