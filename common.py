"""Общие помощники для расширенных инструментов (gitx.py, ops.py). Работают поверх app.py, не меняя его."""
import asyncio
import difflib
import hashlib
from typing import Optional
from urllib.parse import quote

from mcp.server.fastmcp import Context
from starlette.concurrency import run_in_threadpool

from app import API, _connector_token, _resolve_account, http

UPLOADS = "https://uploads.github.com"


async def ghx(ctx: Context, account: str, method: str, path: str, *, write: bool = False, raw: bool = False,
              base: str = API, accept: str = "application/vnd.github+json", headers: Optional[dict] = None, **kw):
    """Как app.gh, но умеет: сырой ответ (raw=True отдаёт httpx.Response), другой хост, свои заголовки.
    write=True для всего, что меняет данные: на ссылках с доступом «только чтение» это блокируется."""
    token = _connector_token(ctx)
    gh_token, readonly, label = await run_in_threadpool(_resolve_account, token, "github", account)
    if write and readonly:
        raise RuntimeError(f"Эта ссылка даёт аккаунту «{label}» только чтение. Права меняются на сайте: «Настроить»")
    h = {"Authorization": f"Bearer {gh_token}", "Accept": accept, "X-GitHub-Api-Version": "2022-11-28",
         "User-Agent": "github-connector-for-claude"}
    if headers:
        h.update(headers)
    r = await http().request(method, f"{base}{path}", headers=h, **kw)
    if r.status_code == 401:
        raise RuntimeError(f"GitHub отклонил токен аккаунта «{label}» (истёк или отозван). Открой сайт коннектора "
                           "и добавь аккаунт заново (та же метка заменит токен).")
    if r.status_code >= 400:
        raise RuntimeError(f"GitHub {r.status_code} (аккаунт {label}): {r.text[:500]}")
    if raw:
        return r
    if r.is_redirect:
        return {"redirect": r.headers.get("location")}
    if not r.content:
        return {}
    try:
        return r.json()
    except ValueError:
        return r.text[:50000]


async def fetch_bytes(url: str, cap: int) -> bytes:
    """Скачать подписанную ссылку (без токена GitHub) с ограничением размера, чтобы не съесть память."""
    buf = bytearray()
    async with http().stream("GET", url, follow_redirects=True) as r:
        if r.status_code >= 400:
            raise RuntimeError(f"Скачивание не удалось: HTTP {r.status_code}")
        async for chunk in r.aiter_bytes():
            buf += chunk
            if len(buf) > cap:
                raise RuntimeError(f"Файл больше лимита {cap // (1024 * 1024)} МБ")
    return bytes(buf)


async def pmap(fn, items, limit: int = 8) -> list:
    """Параллельный map с ограничением числа одновременных запросов."""
    sem = asyncio.Semaphore(limit)

    async def run(x):
        async with sem:
            return await fn(x)

    return list(await asyncio.gather(*[run(x) for x in items]))


def q(path: str) -> str:
    return quote(path, safe="/")


def blob_sha(data: bytes) -> str:
    """Git-хеш содержимого: совпадает с sha блоба на GitHub, поэтому «не изменилось» видно без запросов."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def looks_binary(data: bytes) -> bool:
    return b"\0" in data[:8000]


def clean_path(p) -> str:
    if not isinstance(p, str) or not p.strip():
        raise RuntimeError("Пустой путь файла")
    p = p.strip().replace("\\", "/")
    parts = p.split("/")
    if p.startswith("/") or "\0" in p or any(s in ("", ".", "..") for s in parts) or parts[0].lower() == ".git":
        raise RuntimeError(f"Недопустимый путь: «{p}» (нужен относительный путь без «..» и пустых частей)")
    return p


def unified(old: str, new: str, path: str, max_chars: int = 3000) -> tuple[str, int, int]:
    lines = list(difflib.unified_diff(old.splitlines(), new.splitlines(), f"a/{path}", f"b/{path}",
                                      lineterm="", n=2))
    add = sum(1 for x in lines if x.startswith("+") and not x.startswith("+++"))
    rem = sum(1 for x in lines if x.startswith("-") and not x.startswith("---"))
    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n…(обрезано)"
    return text, add, rem
