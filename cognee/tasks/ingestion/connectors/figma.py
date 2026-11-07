"""Figma connector for cognee: a ``dlt`` source that turns Figma design files, pages, UI components, text elements, and comment threads into memory.

One source ingests one or more Figma files by ``file_key``. For each file it yields:
- A file overview document (``figma:file:<file_key>``) with design metadata, component catalog,
  component sets, styles, and file-level comment threads.
- One document per canvas page (``figma:page:<file_key>:<page_id>``) preserving the file-to-page
  and frame/component hierarchy, all text nodes (``TEXT.characters``), prototype flow links
  (``transitionNodeID``), and comments pinned to nodes on that page.

Usage::

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
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import quote

from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    NODE_SET_COLUMN,
    PIPELINE_SCOPE_ATTR,
)

logger = logging.getLogger(__name__)

FIGMA_API_BASE_URL = "https://api.figma.com/v1"
DEFAULT_MAX_REQUESTS = 300
_TIMEOUT = (10, 30)
_RETRIES = 3
_FILE_KEY_RE = re.compile(r"^[A-Za-z0-9_-]{4,128}$")


class FigmaSourceError(RuntimeError):
    """Base error raised by the Figma connector."""


class FigmaAuthError(FigmaSourceError):
    """Figma rejected the credentials (HTTP 401 or 403)."""


class FigmaRateLimitedError(FigmaSourceError):
    """Figma answered HTTP 429 Too Many Requests."""

    def __init__(self, retry_after: int | None = None):
        super().__init__("Figma rate limit exceeded (HTTP 429)")
        self.retry_after = retry_after


class FigmaAPIError(FigmaSourceError):
    """Figma answered with an unexpected HTTP or payload error."""


def _normalize_file_keys(file_keys: list[str] | tuple[str, ...]) -> list[str]:
    if not isinstance(file_keys, (list, tuple)) or not file_keys:
        raise ValueError("file_keys must be a non-empty list or tuple of Figma file keys.")
    cleaned: list[str] = []
    seen: set[str] = set()
    for key in file_keys:
        if not isinstance(key, str):
            raise ValueError("Every Figma file key must be a string.")
        norm = key.strip()
        if not _FILE_KEY_RE.match(norm):
            raise ValueError(f"Invalid Figma file key: {key!r}")
        if norm not in seen:
            seen.add(norm)
            cleaned.append(norm)
    return cleaned


class FigmaClient:
    """Minimal synchronous REST client for the Figma v1 API."""

    def __init__(
        self,
        access_token: str,
        *,
        base_url: str = FIGMA_API_BASE_URL,
        sleep: Callable[[float], None] = time.sleep,
    ):
        token = (access_token or "").strip()
        if not token or not token.isascii() or not token.isprintable():
            raise ValueError("The Figma access token is empty or has an invalid format.")
        self._access_token = token
        self.base_url = (base_url or FIGMA_API_BASE_URL).strip().rstrip("/")
        self._sleep = sleep

    def __repr__(self) -> str:
        return f"FigmaClient(base_url={self.base_url!r}, access_token=<redacted>)"

    def _headers(self) -> dict[str, str]:
        token = self._access_token
        headers = {
            "Accept": "application/json",
        }
        if token.lower().startswith("bearer "):
            headers["Authorization"] = token
        elif token.startswith("figu_") or token.startswith("oauth_"):
            headers["Authorization"] = f"Bearer {token}"
        else:
            headers["X-Figma-Token"] = token
        return headers

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        import requests

        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(_RETRIES):
            last = attempt == _RETRIES - 1
            try:
                response = requests.request(
                    method,
                    url,
                    headers=self._headers(),
                    params=params,
                    timeout=_TIMEOUT,
                )
            except requests.RequestException as exc:
                if last:
                    raise FigmaAPIError(f"Figma request failed: {type(exc).__name__}") from None
                self._sleep(2**attempt)
                continue

            status = response.status_code
            if status >= 500 and not last:
                self._sleep(2**attempt)
                continue
            if status in (401, 403):
                raise FigmaAuthError(f"Figma rejected credentials (HTTP {status})")
            if status == 429:
                retry_hdr = response.headers.get("Retry-After")
                retry_sec = int(retry_hdr) if retry_hdr and retry_hdr.isdigit() else None
                raise FigmaRateLimitedError(retry_after=retry_sec)
            if status == 404:
                raise FigmaAPIError("Figma file or endpoint not found (HTTP 404)")
            if status >= 400:
                raise FigmaAPIError(f"Figma request failed with HTTP {status}")

            try:
                data = response.json()
            except ValueError as exc:
                raise FigmaAPIError("Figma response is not valid JSON") from exc
            if not isinstance(data, dict):
                raise FigmaAPIError("Figma response root must be a JSON object")
            return data

        raise FigmaAPIError("Figma request retries exhausted")

    def get_file(self, file_key: str) -> dict[str, Any]:
        return self.request_json("GET", f"files/{quote(file_key, safe='')}")

    def get_comments(self, file_key: str) -> list[dict[str, Any]]:
        payload = self.request_json("GET", f"files/{quote(file_key, safe='')}/comments")
        comments = payload.get("comments")
        if not isinstance(comments, list):
            return []
        return [c for c in comments if isinstance(c, dict)]


def build_figma_service(
    access_token: str,
    *,
    base_url: str = FIGMA_API_BASE_URL,
) -> FigmaClient:
    """Construct a :class:`FigmaClient` for ``figma_source``."""
    return FigmaClient(access_token, base_url=base_url)


def _collect_node_ids(node: dict[str, Any], out: set[str]) -> None:
    nid = str(node.get("id") or "").strip()
    if nid:
        out.add(nid)
    children = node.get("children")
    if isinstance(children, list):
        for child in children:
            if isinstance(child, dict):
                _collect_node_ids(child, out)


def _walk_design_tree(
    node: dict[str, Any],
    *,
    path_prefix: tuple[str, ...] = (),
    text_lines: list[str],
    component_lines: list[str],
    prototype_lines: list[str],
) -> None:
    node_type = str(node.get("type") or "").strip().upper()
    node_name = str(node.get("name") or "").strip()
    node_id = str(node.get("id") or "").strip()

    current_path = path_prefix
    if node_type and node_type != "CANVAS" and node_name:
        current_path = (*path_prefix, node_name)

    crumb = " > ".join(current_path) if current_path else node_name or node_id

    if node_type in ("COMPONENT", "COMPONENT_SET", "INSTANCE"):
        comp_id = str(node.get("componentId") or node_id).strip()
        desc = str(node.get("description") or "").strip()
        label = f"- [{node_type}] {crumb} (id={comp_id})"
        if desc:
            label += f": {desc}"
        component_lines.append(label)

    if node_type == "TEXT":
        chars = str(node.get("characters") or "").strip()
        if chars:
            text_lines.append(f"- {crumb}: {chars}")

    transition_target = str(node.get("transitionNodeID") or "").strip()
    if transition_target:
        easing = str((node.get("transitionEasing") or "")).strip()
        proto_str = f"- {crumb} ({node_id}) -> target node {transition_target}"
        if easing:
            proto_str += f" [{easing}]"
        prototype_lines.append(proto_str)

    children = node.get("children")
    if isinstance(children, list):
        for child in children:
            if isinstance(child, dict):
                _walk_design_tree(
                    child,
                    path_prefix=current_path,
                    text_lines=text_lines,
                    component_lines=component_lines,
                    prototype_lines=prototype_lines,
                )


def _format_comment_threads(comments: list[dict[str, Any]]) -> list[str]:
    by_id: dict[str, dict[str, Any]] = {}
    roots: list[dict[str, Any]] = []
    replies_by_parent: dict[str, list[dict[str, Any]]] = {}

    sorted_comments = sorted(
        comments,
        key=lambda c: (str(c.get("created_at") or ""), str(c.get("id") or "")),
    )
    for c in sorted_comments:
        cid = str(c.get("id") or "").strip()
        if cid:
            by_id[cid] = c
        parent_id = str(c.get("parent_id") or "").strip()
        if parent_id:
            replies_by_parent.setdefault(parent_id, []).append(c)
        else:
            roots.append(c)

    lines: list[str] = []
    for root in roots:
        rid = str(root.get("id") or "").strip()
        user = str((root.get("user") or {}).get("handle") or "Designer").strip()
        msg = str(root.get("message") or "").strip()
        if not msg:
            continue
        resolved = " [resolved]" if root.get("resolved_at") else ""
        client_meta = root.get("client_meta") if isinstance(root.get("client_meta"), dict) else {}
        pinned_node = str(client_meta.get("node_id") or "").strip()
        pin_suffix = f" (node {pinned_node})" if pinned_node else ""
        lines.append(f"- {user}{pin_suffix}{resolved}: {msg}")
        for reply in replies_by_parent.get(rid, []):
            r_user = str((reply.get("user") or {}).get("handle") or "Designer").strip()
            r_msg = str(reply.get("message") or "").strip()
            if r_msg:
                lines.append(f"  - {r_user}: {r_msg}")
    return lines


def _comments_digest(comments: list[dict[str, Any]]) -> str:
    canonical = [
        {
            "id": str(c.get("id") or ""),
            "message": str(c.get("message") or ""),
            "resolved_at": str(c.get("resolved_at") or ""),
            "parent_id": str(c.get("parent_id") or ""),
        }
        for c in sorted(comments, key=lambda x: str(x.get("id") or ""))
    ]
    return hashlib.sha256(json.dumps(canonical, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def _render_file_and_page_rows(
    file_key: str,
    file_payload: dict[str, Any],
    comments: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[tuple[str, dict[str, Any]]]]:
    file_name = str(file_payload.get("name") or file_key).strip()
    schema_ver = file_payload.get("schemaVersion")
    editor_type = str(file_payload.get("editorType") or "figma").strip()
    document = file_payload.get("document") if isinstance(file_payload.get("document"), dict) else {}
    raw_children = document.get("children") if isinstance(document.get("children"), list) else []
    pages = [p for p in raw_children if isinstance(p, dict) and str(p.get("type") or "").upper() == "CANVAS"]

    # Map every node_id on each page so comments pinned to a child node attach to its page
    page_node_sets: dict[str, set[str]] = {}
    for page in pages:
        pid = str(page.get("id") or "").strip()
        if pid:
            nset: set[str] = set()
            _collect_node_ids(page, nset)
            page_node_sets[pid] = nset

    # Partition comments into page-specific vs file-level
    comments_by_page: dict[str, list[dict[str, Any]]] = {pid: [] for pid in page_node_sets}
    file_level_comments: list[dict[str, Any]] = []
    parent_to_page: dict[str, str] = {}

    for c in comments:
        parent_id = str(c.get("parent_id") or "").strip()
        if parent_id:
            continue
        cid = str(c.get("id") or "").strip()
        meta = c.get("client_meta") if isinstance(c.get("client_meta"), dict) else {}
        node_id = str(meta.get("node_id") or "").strip()
        assigned_page: str | None = None
        if node_id:
            for pid, nset in page_node_sets.items():
                if node_id in nset:
                    assigned_page = pid
                    break
        if assigned_page is not None:
            comments_by_page[assigned_page].append(c)
            if cid:
                parent_to_page[cid] = assigned_page
        else:
            file_level_comments.append(c)

    for c in comments:
        parent_id = str(c.get("parent_id") or "").strip()
        if not parent_id:
            continue
        target_page = parent_to_page.get(parent_id)
        if target_page is not None:
            comments_by_page[target_page].append(c)
        else:
            file_level_comments.append(c)

    # Render file overview row
    file_lines: list[str] = [
        f"Figma File: {file_name}",
        f"File Key: {file_key}",
        f"Editor Type: {editor_type}",
    ]
    if schema_ver is not None:
        file_lines.append(f"Schema Version: {schema_ver}")

    if pages:
        file_lines.append("")
        file_lines.append("Pages:")
        for p in pages:
            pname = str(p.get("name") or p.get("id") or "Untitled Page").strip()
            pid = str(p.get("id") or "").strip()
            file_lines.append(f"- {pname} (id={pid})")

    components_map = file_payload.get("components") if isinstance(file_payload.get("components"), dict) else {}
    if components_map:
        file_lines.append("")
        file_lines.append("Published Components:")
        for comp_id in sorted(components_map.keys()):
            cmeta = components_map[comp_id]
            if not isinstance(cmeta, dict):
                continue
            cname = str(cmeta.get("name") or comp_id).strip()
            cdesc = str(cmeta.get("description") or "").strip()
            entry = f"- {cname} (id={comp_id})"
            if cdesc:
                entry += f": {cdesc}"
            file_lines.append(entry)

    component_sets_map = (
        file_payload.get("componentSets") if isinstance(file_payload.get("componentSets"), dict) else {}
    )
    if component_sets_map:
        file_lines.append("")
        file_lines.append("Component Sets:")
        for set_id in sorted(component_sets_map.keys()):
            smeta = component_sets_map[set_id]
            if not isinstance(smeta, dict):
                continue
            sname = str(smeta.get("name") or set_id).strip()
            sdesc = str(smeta.get("description") or "").strip()
            entry = f"- {sname} (id={set_id})"
            if sdesc:
                entry += f": {sdesc}"
            file_lines.append(entry)

    styles_map = file_payload.get("styles") if isinstance(file_payload.get("styles"), dict) else {}
    if styles_map:
        file_lines.append("")
        file_lines.append("Design Styles:")
        for style_id in sorted(styles_map.keys()):
            stmeta = styles_map[style_id]
            if not isinstance(stmeta, dict):
                continue
            stname = str(stmeta.get("name") or style_id).strip()
            sttype = str(stmeta.get("styleType") or "").strip()
            stdesc = str(stmeta.get("description") or "").strip()
            entry = f"- {stname}" + (f" [{sttype}]" if sttype else "")
            if stdesc:
                entry += f": {stdesc}"
            file_lines.append(entry)

    file_comment_lines = _format_comment_threads(file_level_comments)
    if file_comment_lines:
        file_lines.append("")
        file_lines.append("Comments:")
        file_lines.extend(file_comment_lines)

    file_url = f"https://www.figma.com/file/{file_key}"
    file_row = {
        "id": f"figma:file:{file_key}",
        "title": f"{file_name} (Figma File)",
        "content": "\n".join(file_lines).strip(),
        "url": file_url,
        "_deleted": False,
        NODE_SET_COLUMN: [f"figma:{file_key}"],
    }

    page_rows: list[tuple[str, dict[str, Any]]] = []
    for page in pages:
        pid = str(page.get("id") or "").strip()
        if not pid:
            continue
        pname = str(page.get("name") or pid).strip()
        text_lines: list[str] = []
        comp_lines: list[str] = []
        proto_lines: list[str] = []

        _walk_design_tree(
            page,
            text_lines=text_lines,
            component_lines=comp_lines,
            prototype_lines=proto_lines,
        )

        p_lines: list[str] = [
            f"Figma Page: {pname}",
            f"File: {file_name} ({file_key})",
            f"Page ID: {pid}",
        ]
        if comp_lines:
            p_lines.append("")
            p_lines.append("UI Components & Instances:")
            p_lines.extend(comp_lines)
        if text_lines:
            p_lines.append("")
            p_lines.append("Text Elements:")
            p_lines.extend(text_lines)
        if proto_lines:
            p_lines.append("")
            p_lines.append("Prototype Flows:")
            p_lines.extend(proto_lines)

        p_comment_lines = _format_comment_threads(comments_by_page.get(pid, []))
        if p_comment_lines:
            p_lines.append("")
            p_lines.append("Page Comments:")
            p_lines.extend(p_comment_lines)

        page_url = f"https://www.figma.com/file/{file_key}?node-id={quote(pid, safe='')}"
        page_rows.append(
            (
                pid,
                {
                    "id": f"figma:page:{file_key}:{pid}",
                    "title": f"{file_name} — {pname}",
                    "content": "\n".join(p_lines).strip(),
                    "url": page_url,
                    "_deleted": False,
                    NODE_SET_COLUMN: [f"figma:{file_key}"],
                },
            )
        )

    return file_row, page_rows


def _iter_rows(
    client: FigmaClient,
    file_keys: list[str],
    state: dict[str, Any],
    stats: dict[str, int],
    *,
    include_comments: bool = True,
    max_requests: int = DEFAULT_MAX_REQUESTS,
) -> Iterator[dict[str, Any]]:
    files_state = state.setdefault("files", {})
    requests_used = 0

    # Tombstone any previously synced Figma files that were deselected
    deselected = sorted(set(files_state.keys()) - set(file_keys))
    for old_key in deselected:
        old_entry = files_state.pop(old_key, {})
        stats["deleted"] += 1
        yield {
            "id": f"figma:file:{old_key}",
            "title": "",
            "content": "",
            "url": "",
            "_deleted": True,
        }
        for old_pid in sorted(old_entry.get("page_ids", [])):
            stats["deleted"] += 1
            yield {
                "id": f"figma:page:{old_key}:{old_pid}",
                "title": "",
                "content": "",
                "url": "",
                "_deleted": True,
            }

    for file_key in file_keys:
        if requests_used >= max_requests:
            stats["failed"] += 1
            stats["failed_budget"] += 1
            return

        try:
            requests_used += 1
            file_payload = client.get_file(file_key)
        except FigmaRateLimitedError:
            stats["failed"] += 1
            stats["failed_rate_limit"] += 1
            return
        except FigmaAuthError:
            if stats["synced"] > 0 or stats["deleted"] > 0:
                stats["failed"] += 1
                stats["failed_auth"] += 1
                return
            raise

        comments: list[dict[str, Any]] = []
        if include_comments:
            if requests_used >= max_requests:
                stats["failed"] += 1
                stats["failed_budget"] += 1
                return
            try:
                requests_used += 1
                comments = client.get_comments(file_key)
            except FigmaRateLimitedError:
                stats["failed"] += 1
                stats["failed_rate_limit"] += 1
                return
            except FigmaAuthError:
                if stats["synced"] > 0 or stats["deleted"] > 0:
                    stats["failed"] += 1
                    stats["failed_auth"] += 1
                    return
                raise

        version = str(file_payload.get("version") or "").strip()
        last_modified = str(file_payload.get("lastModified") or "").strip()
        c_digest = _comments_digest(comments) if include_comments else ""

        f_state = files_state.setdefault(
            file_key,
            {
                "version": None,
                "last_modified": None,
                "comments_digest": None,
                "page_ids": [],
            },
        )

        # Incremental skip if neither file version/lastModified nor comments changed
        if (
            f_state.get("version") == version
            and f_state.get("last_modified") == last_modified
            and f_state.get("comments_digest") == c_digest
            and (version or last_modified)
        ):
            continue

        file_row, page_rows = _render_file_and_page_rows(file_key, file_payload, comments)
        stats["synced"] += 1
        yield file_row

        current_pids: set[str] = set()
        for pid, prow in page_rows:
            current_pids.add(pid)
            stats["synced"] += 1
            yield prow

        # Tombstone pages deleted from the file since the previous sync
        previous_pids = set(f_state.get("page_ids", []))
        for removed_pid in sorted(previous_pids - current_pids):
            stats["deleted"] += 1
            yield {
                "id": f"figma:page:{file_key}:{removed_pid}",
                "title": "",
                "content": "",
                "url": "",
                "_deleted": True,
            }

        f_state["version"] = version
        f_state["last_modified"] = last_modified
        f_state["comments_digest"] = c_digest
        f_state["page_ids"] = sorted(current_pids)


def figma_source(
    *,
    file_keys: list[str] | tuple[str, ...],
    access_token: str | None = None,
    service: FigmaClient | None = None,
    resource_name: str | None = None,
    include_comments: bool = True,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    check_active: Callable[[], None] | None = None,
):
    """Return a ``dlt`` resource that yields Figma file and page documents across ``file_keys``."""
    import dlt

    if getattr(dlt_utils, "DOCUMENT_SYNC_VERSION", 0) < 3:
        raise RuntimeError(
            "Figma sync requires a Cognee build that reads per-row node sets and "
            "structure (DOCUMENT_SYNC_VERSION >= 3). Upgrade Cognee before syncing."
        )

    keys = _normalize_file_keys(file_keys)
    if max_requests < 1:
        raise ValueError("max_requests must be at least 1.")

    if service is not None:
        client = service
    elif access_token is not None:
        client = build_figma_service(access_token)
    else:
        raise ValueError("Either access_token or service must be provided.")

    if resource_name is None:
        digest = hashlib.sha256("|".join(sorted(keys)).encode("utf-8")).hexdigest()[:12]
        resource_name = f"figma_{digest}"
    elif not isinstance(resource_name, str) or not resource_name.strip():
        raise ValueError("resource_name must be a non-empty string.")

    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        columns={
            "_deleted": {"data_type": "bool", "hard_delete": True},
            NODE_SET_COLUMN: {"data_type": "json", "nullable": True},
        },
    )
    def figma_files():
        stats.update(
            synced=0,
            deleted=0,
            failed=0,
            failed_rate_limit=0,
            failed_budget=0,
            failed_auth=0,
        )
        rows = _iter_rows(
            client,
            keys,
            dlt.current.resource_state(),
            stats,
            include_comments=include_comments,
            max_requests=max_requests,
        )
        yield from dlt_utils.guarded_rows(rows, check_active)

    resource = figma_files()
    setattr(resource, DOCUMENT_SOURCE_ATTR, "figma")
    setattr(resource, PIPELINE_SCOPE_ATTR, resource_name)
    resource.cognee_sync_stats = stats
    return resource
