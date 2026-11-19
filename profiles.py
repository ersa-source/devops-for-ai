"""Метки инструментов (readOnlyHint и др.) и фильтр набора инструментов по ссылке коннектора."""
import json
import time

from mcp.types import CallToolRequest, CallToolResult, ListToolsRequest, ServerResult, TextContent, ToolAnnotations
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

import app
import toolsets as TS

# ---- метки ----
for _name, _tool in app.mcp._tool_manager._tools.items():
    _tool.annotations = ToolAnnotations(**TS.annotation_for(_name))

# ---- фильтр по ссылке ----
_cache: dict = {}  # token -> (время, allowed|None)
TTL = 30


def _lookup(token: str):
    with Session(app.engine) as s:
        c = s.scalar(select(app.Connector).where(app.Connector.token_hash == app.sha(token)))
        if not c or c.revoked:
            return None
        return TS.allowed_tools(c.toolsets)


async def _allowed():
    """Набор разрешённых имён для текущего запроса; None = без ограничений."""
    try:
        req = app.mcp._mcp_server.request_context.request
        token = req.scope.get("mcp_token") if req is not None else None
    except Exception:  # noqa: BLE001
        return None
    if not token:
        return None
    hit = _cache.get(token)
    if hit and time.time() - hit[0] < TTL:
        return hit[1]
    val = await run_in_threadpool(_lookup, token)
    _cache[token] = (time.time(), val)
    return val


_srv = app.mcp._mcp_server
_orig_list = _srv.request_handlers[ListToolsRequest]
_orig_call = _srv.request_handlers[CallToolRequest]


async def _list(req):
    res = await _orig_list(req)
    allowed = await _allowed()
    if allowed is not None:
        res.root.tools = [t for t in res.root.tools if t.name in allowed]
    return res


async def _call(req):
    allowed = await _allowed()
    if allowed is not None and req.params.name not in allowed:
        return ServerResult(CallToolResult(isError=True, content=[TextContent(
            type="text", text=f"Инструмент «{req.params.name}» отключён в настройках этой ссылки. "
                              "Включить: сайт коннектора, «Настроить» у ссылки.")]))
    return await _orig_call(req)


_srv.request_handlers[ListToolsRequest] = _list
_srv.request_handlers[CallToolRequest] = _call

# сброс кэша вместе с кэшем аккаунтов (отзыв, правка ссылки)
_orig_inval = app.invalidate_cache


def _inval(*a, **k):
    _cache.clear()
    return _orig_inval(*a, **k)


app.invalidate_cache = _inval
