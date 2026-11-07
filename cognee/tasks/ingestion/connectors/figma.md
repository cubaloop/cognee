# Figma Data-Source Connector (`figma_source`)

Ingests Figma design files, canvas pages, UI components, component sets, styles, text elements, prototype flow targets, and comment threads as incremental, deletion-aware document rows for `cognee.remember(...)`.

## Usage

```python
import cognee
from cognee.tasks.ingestion.connectors import figma_source

await cognee.remember(
    figma_source(
        file_keys=["AbCdEf123456"],
        access_token="figd_...",
    ),
    dataset_name="figma",
    primary_key="id",
    write_disposition="merge",
    max_rows_per_table=0,
)
```

## Features

- **Document-mode DLT Resource**: Tagged with `cognee_document_source = "figma"` and `write_disposition = "merge"`.
- **File & Page Hierarchy**: Emits one file overview document (`figma:file:<file_key>`) and one document per canvas page (`figma:page:<file_key>:<page_id>`), preserving frame/section breadcrumb paths for UI components, text elements (`TEXT.characters`), and prototype transitions (`transitionNodeID`).
- **Comment Thread Extraction**: Fetches file comments (`GET /v1/files/{key}/comments`) and attaches threads pinned to nodes directly to their corresponding page document.
- **Incremental & Tombstone Sync**: Tracks `version`, `lastModified`, and a comment thread digest per file in DLT resource state; emits `_deleted = True` tombstones when pages are removed or files are deselected.
- **Bounded & Resumable**: Stops cleanly on HTTP 429 rate limits or `max_requests` budget limits while recording diagnostics in `resource.cognee_sync_stats`.
