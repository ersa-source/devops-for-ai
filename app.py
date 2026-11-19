"""GitHub Connector for Claude: сайт + MCP-сервер в одном приложении (Railway).

Поток: вход через Google -> аккаунты GitHub/Railway (сколько угодно) -> создание ссылки коннектора
-> вставка ссылки в Claude (Settings -> Connectors -> Add custom connector).
"""
import base64
import hashlib
import html
import json
import os
import re
import secrets
from contextlib import asynccontextmanager
import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlencode

import httpx
from cryptography.fernet import Fernet
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from mcp.server.fastmcp import Context, FastMCP
from sqlalchemy import (Boolean, DateTime, ForeignKey, Integer, String, create_engine, func, inspect,
                        or_, select, text)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from starlette.concurrency import run_in_threadpool
from starlette.middleware.sessions import SessionMiddleware

# ---------------------------------------------------------------- config
def env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise RuntimeError(f"Не задана переменная окружения {name}")
    return v


BASE_URL = env("BASE_URL").rstrip("/")
SECRET_KEY = env("SECRET_KEY")
GOOGLE_CLIENT_ID = env("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = env("GOOGLE_CLIENT_SECRET")
GITHUB_CLIENT_ID = env("GITHUB_CLIENT_ID")
GITHUB_CLIENT_SECRET = env("GITHUB_CLIENT_SECRET")
ALLOWED_EMAILS = {e.strip().lower() for e in os.environ.get("ALLOWED_EMAILS", "").split(",") if e.strip()}

DB_URL = os.environ.get("DATABASE_URL", "sqlite:///./app.db")
if DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)
if DB_URL.startswith("postgresql://"):  # явно указываем драйвер: в SQLAlchemy 2.1 по умолчанию psycopg v3
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

fernet = Fernet(env("ENCRYPTION_KEY"))


def now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------- database
class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    google_sub: Mapped[str] = mapped_column(String(64), unique=True)
    email: Mapped[str] = mapped_column(String(255))
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    gh_login: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    gh_token_enc: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now)


class Connector(Base):
    __tablename__ = "connectors"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String(100))
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    readonly: Mapped[bool] = mapped_column(Boolean, default=False)
    revoked: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now)
    last_used: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    # NULL = ссылка видит все аккаунты (права по readonly); иначе JSON {"id аккаунта": "r"|"w"}
    perms: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    # NULL = все инструменты; иначе JSON-список групп (см. toolsets.py)
    toolsets: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class Account(Base):
    """Подключённые внешние аккаунты: GitHub и Railway, сколько угодно. label — короткое имя для Claude."""
    __tablename__ = "accounts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    kind: Mapped[str] = mapped_column(String(20))
    label: Mapped[str] = mapped_column(String(100), default="main")
    login: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    token_enc: Mapped[str] = mapped_column(String)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now)


engine = create_engine(DB_URL, pool_pre_ping=True)
Base.metadata.create_all(engine)


def _migrate() -> None:
    """create_all не меняет существующие таблицы, поэтому колонку добавляем сами; старый GitHub-токен
    из users переезжает в accounts (один раз), у каждого типа аккаунтов должен быть основной."""
    if "is_default" not in {c["name"] for c in inspect(engine).get_columns("accounts")}:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE accounts ADD COLUMN is_default BOOLEAN DEFAULT FALSE"))
    if "perms" not in {c["name"] for c in inspect(engine).get_columns("connectors")}:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE connectors ADD COLUMN perms TEXT"))
    if "toolsets" not in {c["name"] for c in inspect(engine).get_columns("connectors")}:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE connectors ADD COLUMN toolsets TEXT"))
    with Session(engine) as s:
        for u in s.scalars(select(User).where(User.gh_token_enc.is_not(None))):
            has = s.scalar(select(Account.id).where(Account.user_id == u.id, Account.kind == "github"))
            if not has:
                s.add(Account(user_id=u.id, kind="github", label=u.gh_login or "main", login=u.gh_login,
                              token_enc=u.gh_token_enc, is_default=True))
            u.gh_token_enc = None  # дальше источник правды только accounts
        s.flush()
        groups: dict = {}
        for a in s.scalars(select(Account).order_by(Account.id)):
            groups.setdefault((a.user_id, a.kind), []).append(a)
        for accs in groups.values():
            if not any(a.is_default for a in accs):
                accs[0].is_default = True
        s.commit()


_migrate()


def sha(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


# ---------------------------------------------------------------- MCP
API = "https://api.github.com"
mcp = FastMCP(
    "github", host="0.0.0.0", stateless_http=True, streamable_http_path="/mcp",
    instructions=("Подключено несколько аккаунтов GitHub и Railway. У каждого инструмента есть параметр account "
                  "(метка аккаунта); без него используется основной. Список меток: list_accounts. "
                  "Вызовы к разным аккаунтам независимы: делай их параллельно."))


_CACHE_TTL = 30  # секунд: столько держим расшифрованный токен в памяти, чтобы не ходить в БД на каждый вызов
_LAST_USED_EVERY = timedelta(minutes=5)  # как часто обновлять «последнее использование»
_cache: dict[str, tuple[float, str, bool, str]] = {}  # ключ -> (время, токен, readonly, метка аккаунта)


def invalidate_cache() -> None:
    """Вызывается при отзыве ссылки или смене аккаунтов (сервис работает в одном процессе)."""
    _cache.clear()


_KIND_RU = {"github": "GitHub", "railway": "Railway"}


def _perms(c) -> Optional[dict]:
    """None = ссылка видит все аккаунты (старый режим, права по c.readonly); иначе {id аккаунта: 'r'|'w'}."""
    if not c.perms:
        return None
    try:
        return {int(k): v for k, v in json.loads(c.perms).items()}
    except (ValueError, AttributeError):
        return {}  # битые данные = доступа нет (безопасный вариант)


def _visible(accs: list, c) -> list:
    """[(аккаунт, можно_писать)] — что видит эта ссылка."""
    p = _perms(c)
    if p is None:
        return [(a, not c.readonly) for a in accs]
    return [(a, p[a.id] == "w") for a in accs if p.get(a.id) in ("r", "w")]


def _pick(vis: list, label: str, kind: str):
    if not vis:
        raise RuntimeError(f"У этой ссылки нет доступа к аккаунтам {_KIND_RU[kind]}: "
                           "открой сайт коннектора и нажми «Настроить»")
    if label:
        want = label.strip().lower()
        for a, w in vis:
            if a.label.lower() == want or (a.login or "").lower() == want:
                return a, w
        raise RuntimeError(f"Аккаунт {_KIND_RU[kind]} «{label}» не найден или недоступен этой ссылке. Доступные: "
                           + ", ".join(a.label for a, _ in vis) + ". Список: инструмент list_accounts")
    return next(((a, w) for a, w in vis if a.is_default), vis[0])


def _resolve_account(token: str, kind: str, label: str = "") -> tuple[str, bool, str]:
    """(токен аккаунта, readonly, метка). Пустая label = основной из доступных этой ссылке."""
    key = f"{kind}:{label.strip().lower()}:{sha(token)}"
    hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < _CACHE_TTL:
        return hit[1], hit[2], hit[3]
    with Session(engine) as s:
        c = s.scalar(select(Connector).where(Connector.token_hash == sha(token), Connector.revoked.is_(False)))
        if not c:
            _cache.pop(key, None)
            raise RuntimeError("Коннектор не найден или отозван")
        accs = s.scalars(select(Account).where(Account.user_id == c.user_id, Account.kind == kind)
                         .order_by(Account.id)).all()
        if not accs:
            raise RuntimeError(f"{_KIND_RU[kind]} не подключён: добавь аккаунт на сайте коннектора")
        acc, can_write = _pick(_visible(accs, c), label, kind)
        tok = fernet.decrypt(acc.token_enc.encode()).decode()
        readonly, acc_label = not can_write, acc.label
        if c.last_used is None or now() - c.last_used > _LAST_USED_EVERY:
            c.last_used = now()
            s.commit()
    _cache[key] = (time.monotonic(), tok, readonly, acc_label)
    return tok, readonly, acc_label


def _list_accounts(token: str) -> list[dict]:
    with Session(engine) as s:
        c = s.scalar(select(Connector).where(Connector.token_hash == sha(token), Connector.revoked.is_(False)))
        if not c:
            raise RuntimeError("Коннектор не найден или отозван")
        rows = s.scalars(select(Account).where(Account.user_id == c.user_id).order_by(Account.id)).all()
        out = []
        for kind in ("github", "railway"):
            vis = _visible([a for a in rows if a.kind == kind], c)
            if not vis:
                continue
            dflt = _pick(vis, "", kind)[0].id
            out += [{"kind": kind, "label": a.label, "login": a.login, "default": a.id == dflt,
                     "access": "read_write" if w else "read"} for a, w in vis]
        return out


_client: Optional[httpx.AsyncClient] = None


def http() -> httpx.AsyncClient:
    """Один общий клиент: соединения с GitHub переиспользуются между вызовами."""
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=30, limits=httpx.Limits(max_connections=50, max_keepalive_connections=20))
    return _client


async def gh(ctx: Context, account: str, method: str, path: str, *, write: bool = False, **kw):
    req = ctx.request_context.request
    token = req.scope.get("mcp_token") if req is not None else None
    if not token:
        raise RuntimeError("Нет токена коннектора")
    gh_token, readonly, label = await run_in_threadpool(_resolve_account, token, "github", account)
    if write and readonly:
        raise RuntimeError(f"Эта ссылка даёт аккаунту «{label}» только чтение. Права меняются на сайте: «Настроить»")
    headers = {
        "Authorization": f"Bearer {gh_token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "github-connector-for-claude",
    }
    r = await http().request(method, f"{API}{path}", headers=headers, **kw)
    if r.status_code == 401:
        raise RuntimeError(f"GitHub отклонил токен аккаунта «{label}» (истёк или отозван). Открой сайт коннектора "
                           "и добавь аккаунт заново (та же метка заменит токен); ссылка коннектора останется прежней.")
    if r.status_code >= 400:
        raise RuntimeError(f"GitHub {r.status_code} (аккаунт {label}): {r.text[:500]}")
    if r.is_redirect:  # например, ссылка на логи Actions
        return {"redirect": r.headers.get("location")}
    if not r.content:
        return {}
    try:
        return r.json()
    except ValueError:  # не JSON (diff, patch и т.п.)
        return r.text[:50000]


@mcp.tool()
async def list_repos(ctx: Context, per_page: int = 30, account: str = "") -> list[dict]:
    """Репозитории пользователя (последние обновлённые)."""
    data = await gh(ctx, account, "GET", "/user/repos", params={"per_page": per_page, "sort": "updated"})
    return [{"full_name": r["full_name"], "private": r["private"], "description": r["description"],
             "default_branch": r["default_branch"], "updated_at": r["updated_at"]} for r in data]


@mcp.tool()
async def search_repos(ctx: Context, query: str, per_page: int = 10, account: str = "") -> list[dict]:
    """Поиск репозиториев (синтаксис GitHub search)."""
    data = await gh(ctx, account, "GET", "/search/repositories", params={"q": query, "per_page": per_page})
    return [{"full_name": r["full_name"], "description": r["description"],
             "stars": r["stargazers_count"]} for r in data["items"]]


@mcp.tool()
async def list_dir(ctx: Context, owner: str, repo: str, path: str = "", ref: str = "", account: str = "") -> list[dict]:
    """Содержимое папки репозитория."""
    data = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/contents/{path}", params={"ref": ref} if ref else {})
    if isinstance(data, dict):
        data = [data]
    return [{"name": i["name"], "path": i["path"], "type": i["type"], "size": i["size"]} for i in data]


@mcp.tool()
async def get_file(ctx: Context, owner: str, repo: str, path: str, ref: str = "", account: str = "") -> str:
    """Прочитать текстовый файл."""
    data = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/contents/{path}", params={"ref": ref} if ref else {})
    if isinstance(data, list):
        raise RuntimeError("Это папка, используй list_dir")
    return base64.b64decode(data["content"]).decode("utf-8", errors="replace")


@mcp.tool()
async def put_file(ctx: Context, owner: str, repo: str, path: str, content: str, message: str,
                   branch: str = "", account: str = "") -> dict:
    """Создать или обновить файл (делает коммит)."""
    body: dict = {"message": message, "content": base64.b64encode(content.encode()).decode()}
    if branch:
        body["branch"] = branch
    try:
        existing = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/contents/{path}",
                            params={"ref": branch} if branch else {})
        body["sha"] = existing["sha"]
    except RuntimeError as e:
        if "404" not in str(e):
            raise
    # проверка режима только-чтение происходит здесь
    data = await gh(ctx, account, "PUT", f"/repos/{owner}/{repo}/contents/{path}", write=True, json=body)
    return {"commit": data["commit"]["sha"], "url": data["commit"]["html_url"]}


@mcp.tool()
async def search_code(ctx: Context, query: str, per_page: int = 10, account: str = "") -> list[dict]:
    """Поиск по коду, например: 'repo:owner/name def main'."""
    data = await gh(ctx, account, "GET", "/search/code", params={"q": query, "per_page": per_page})
    return [{"repo": i["repository"]["full_name"], "path": i["path"], "url": i["html_url"]} for i in data["items"]]


@mcp.tool()
async def list_commits(ctx: Context, owner: str, repo: str, per_page: int = 10, sha: str = "", account: str = "") -> list[dict]:
    """Последние коммиты (опционально для ветки/sha)."""
    params: dict = {"per_page": per_page}
    if sha:
        params["sha"] = sha
    data = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/commits", params=params)
    return [{"sha": c["sha"][:7], "message": c["commit"]["message"].split("\n")[0],
             "author": c["commit"]["author"]["name"], "date": c["commit"]["author"]["date"]} for c in data]


@mcp.tool()
async def list_issues(ctx: Context, owner: str, repo: str, state: str = "open", per_page: int = 20, account: str = "") -> list[dict]:
    """Issues репозитория (state: open/closed/all)."""
    data = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/issues", params={"state": state, "per_page": per_page})
    return [{"number": i["number"], "title": i["title"], "state": i["state"],
             "is_pr": "pull_request" in i, "url": i["html_url"]} for i in data]


@mcp.tool()
async def get_issue(ctx: Context, owner: str, repo: str, number: int, account: str = "") -> dict:
    """Issue или PR с текстом и комментариями."""
    issue = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/issues/{number}")
    comments = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/issues/{number}/comments")
    return {"title": issue["title"], "state": issue["state"], "body": issue["body"],
            "comments": [{"user": c["user"]["login"], "body": c["body"]} for c in comments]}


@mcp.tool()
async def create_issue(ctx: Context, owner: str, repo: str, title: str, body: str = "", account: str = "") -> dict:
    """Создать issue."""
    i = await gh(ctx, account, "POST", f"/repos/{owner}/{repo}/issues", write=True, json={"title": title, "body": body})
    return {"number": i["number"], "url": i["html_url"]}


@mcp.tool()
async def comment_issue(ctx: Context, owner: str, repo: str, number: int, body: str, account: str = "") -> dict:
    """Комментарий к issue/PR."""
    c = await gh(ctx, account, "POST", f"/repos/{owner}/{repo}/issues/{number}/comments", write=True, json={"body": body})
    return {"url": c["html_url"]}


@mcp.tool()
async def list_pulls(ctx: Context, owner: str, repo: str, state: str = "open", account: str = "") -> list[dict]:
    """Pull request'ы репозитория."""
    data = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/pulls", params={"state": state})
    return [{"number": p["number"], "title": p["title"], "head": p["head"]["ref"],
             "base": p["base"]["ref"], "url": p["html_url"]} for p in data]


@mcp.tool()
async def create_pull(ctx: Context, owner: str, repo: str, title: str, head: str, base: str, body: str = "", account: str = "") -> dict:
    """Создать pull request."""
    p = await gh(ctx, account, "POST", f"/repos/{owner}/{repo}/pulls", write=True,
                 json={"title": title, "head": head, "base": base, "body": body})
    return {"number": p["number"], "url": p["html_url"]}


@mcp.tool()
async def list_branches(ctx: Context, owner: str, repo: str, per_page: int = 30, account: str = "") -> list[dict]:
    """Ветки репозитория."""
    data = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/branches", params={"per_page": per_page})
    return [{"name": b["name"], "sha": b["commit"]["sha"][:7], "protected": b["protected"]} for b in data]


@mcp.tool()
async def create_branch(ctx: Context, owner: str, repo: str, branch: str, from_ref: str = "", account: str = "") -> dict:
    """Создать ветку от другой ветки (по умолчанию от основной ветки репозитория)."""
    if not from_ref:
        from_ref = (await gh(ctx, account, "GET", f"/repos/{owner}/{repo}"))["default_branch"]
    base = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/git/ref/heads/{from_ref}")
    r = await gh(ctx, account, "POST", f"/repos/{owner}/{repo}/git/refs", write=True,
                 json={"ref": f"refs/heads/{branch}", "sha": base["object"]["sha"]})
    return {"ref": r["ref"], "sha": r["object"]["sha"][:7], "from": from_ref}


@mcp.tool()
async def delete_file(ctx: Context, owner: str, repo: str, path: str, message: str, branch: str = "", account: str = "") -> dict:
    """Удалить файл (делает коммит)."""
    params = {"ref": branch} if branch else {}
    existing = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/contents/{path}", params=params)
    if isinstance(existing, list):
        raise RuntimeError("Это папка, удаляются только файлы")
    body: dict = {"message": message, "sha": existing["sha"]}
    if branch:
        body["branch"] = branch
    data = await gh(ctx, account, "DELETE", f"/repos/{owner}/{repo}/contents/{path}", write=True, json=body)
    return {"commit": data["commit"]["sha"], "url": data["commit"]["html_url"]}


@mcp.tool()
async def get_pull(ctx: Context, owner: str, repo: str, number: int, account: str = "") -> dict:
    """PR: описание, статус и список изменённых файлов."""
    p = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/pulls/{number}")
    files = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/pulls/{number}/files", params={"per_page": 100})
    return {"title": p["title"], "state": p["state"], "merged": p["merged"], "mergeable": p["mergeable"],
            "head": p["head"]["ref"], "base": p["base"]["ref"], "body": p["body"],
            "files": [{"filename": f["filename"], "status": f["status"], "additions": f["additions"],
                       "deletions": f["deletions"], "patch": (f.get("patch") or "")[:3000]} for f in files]}


@mcp.tool()
async def merge_pull(ctx: Context, owner: str, repo: str, number: int, method: str = "merge",
                     commit_title: str = "", account: str = "") -> dict:
    """Смержить PR (method: merge, squash или rebase)."""
    body: dict = {"merge_method": method}
    if commit_title:
        body["commit_title"] = commit_title
    r = await gh(ctx, account, "PUT", f"/repos/{owner}/{repo}/pulls/{number}/merge", write=True, json=body)
    return {"merged": r.get("merged"), "message": r.get("message"), "sha": (r.get("sha") or "")[:7]}


@mcp.tool()
async def list_workflow_runs(ctx: Context, owner: str, repo: str, branch: str = "", per_page: int = 10, account: str = "") -> list[dict]:
    """Последние запуски GitHub Actions."""
    params: dict = {"per_page": per_page}
    if branch:
        params["branch"] = branch
    data = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/actions/runs", params=params)
    return [{"id": r["id"], "name": r["name"], "status": r["status"], "conclusion": r["conclusion"],
             "branch": r["head_branch"], "created_at": r["created_at"], "url": r["html_url"]}
            for r in data["workflow_runs"]]


@mcp.tool()
async def get_run_jobs(ctx: Context, owner: str, repo: str, run_id: int, account: str = "") -> list[dict]:
    """Джобы и шаги запуска Actions (видно, на каком шаге упало)."""
    data = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/actions/runs/{run_id}/jobs")
    return [{"id": j["id"], "name": j["name"], "status": j["status"], "conclusion": j["conclusion"],
             "steps": [{"name": s["name"], "conclusion": s["conclusion"]} for s in j["steps"]]}
            for j in data["jobs"]]


@mcp.tool()
async def get_job_logs(ctx: Context, owner: str, repo: str, job_id: int, tail_lines: int = 200, account: str = "") -> str:
    """Лог джобы Actions (последние строки)."""
    r = await gh(ctx, account, "GET", f"/repos/{owner}/{repo}/actions/jobs/{job_id}/logs")
    url = r.get("redirect") if isinstance(r, dict) else None
    if not url:
        return str(r)[-20000:]
    text = (await http().get(url)).text  # ссылка уже подписана, токен не нужен
    return "\n".join(text.splitlines()[-tail_lines:])[-30000:]


@mcp.tool()
async def dispatch_workflow(ctx: Context, owner: str, repo: str, workflow: str, ref: str,
                            inputs: Optional[dict] = None, account: str = "") -> dict:
    """Запустить workflow вручную (workflow: имя файла, например ci.yml; ref: ветка или тег)."""
    body: dict = {"ref": ref}
    if inputs:
        body["inputs"] = inputs
    await gh(ctx, account, "POST", f"/repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches", write=True, json=body)
    return {"dispatched": True, "workflow": workflow, "ref": ref}


@mcp.tool()
async def list_accounts(ctx: Context) -> list[dict]:
    """Подключённые аккаунты GitHub и Railway: kind, label (его передавать в параметр account), login, default."""
    return await run_in_threadpool(_list_accounts, _connector_token(ctx))


@mcp.tool()
async def github_api(ctx: Context, method: str, path: str, params: Optional[dict] = None,
                     body: Optional[dict] = None, account: str = ""):
    """Любой запрос к GitHub REST API. method: GET/POST/PUT/PATCH/DELETE, path вида /repos/OWNER/REPO/releases.
    В режиме «только чтение» разрешён лишь GET. Документация: docs.github.com/rest"""
    method = method.upper()
    if method not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
        raise RuntimeError("Неподдерживаемый метод")
    if not path.startswith("/") or "://" in path or path.startswith("//"):
        raise RuntimeError("path должен начинаться с / и быть относительным (например, /user)")
    kw: dict = {}
    if params:
        kw["params"] = params
    if body is not None:
        kw["json"] = body
    return await gh(ctx, account, method, path, write=method != "GET", **kw)


@mcp.tool()
async def repo_overview(ctx: Context, owner: str, repo: str, commits: int = 5, issues: int = 10,
                        pulls: int = 10, runs: int = 5, account: str = "") -> dict:
    """Общий обзор репозитория одним вызовом: описание, ветки, последние коммиты, открытые issues и PR,
    запуски Actions. Используй вместо десятка отдельных вызовов. Внутри всё запрашивается параллельно."""
    base = f"/repos/{owner}/{repo}"

    async def part(path: str, **params):
        try:
            return await gh(ctx, account, "GET", f"{base}{path}", params=params or None)
        except RuntimeError as e:  # одна упавшая часть (например, Actions выключены) не ломает весь обзор
            return {"error": str(e)[:200]}

    info, branches, cms, iss, prs, wf = await asyncio.gather(
        part(""), part("/branches", per_page=30), part("/commits", per_page=commits),
        part("/issues", state="open", per_page=issues + pulls), part("/pulls", state="open", per_page=pulls),
        part("/actions/runs", per_page=runs),
    )

    def ok(x):
        return isinstance(x, list)

    out: dict = {}
    if isinstance(info, dict) and "error" not in info:
        out["repo"] = {k: info.get(k) for k in ("full_name", "description", "private", "default_branch",
                                               "language", "open_issues_count", "updated_at", "pushed_at")}
    else:
        out["repo"] = info
    out["branches"] = ([{"name": b["name"], "protected": b["protected"]} for b in branches]
                       if ok(branches) else branches)
    out["commits"] = ([{"sha": c["sha"][:7], "message": c["commit"]["message"].split("\n")[0],
                        "author": c["commit"]["author"]["name"], "date": c["commit"]["author"]["date"]}
                       for c in cms] if ok(cms) else cms)
    out["issues"] = ([{"number": i["number"], "title": i["title"], "url": i["html_url"]}
                      for i in iss if "pull_request" not in i][:issues] if ok(iss) else iss)
    out["pulls"] = ([{"number": p["number"], "title": p["title"], "head": p["head"]["ref"],
                      "base": p["base"]["ref"], "url": p["html_url"]} for p in prs] if ok(prs) else prs)
    if isinstance(wf, dict) and "workflow_runs" in wf:
        out["actions"] = [{"id": r["id"], "name": r["name"], "status": r["status"],
                           "conclusion": r["conclusion"], "branch": r["head_branch"]}
                          for r in wf["workflow_runs"]]
    else:
        out["actions"] = wf
    return out


# ---------------------------------------------------------------- Railway
RAILWAY_URL = "https://backboard.railway.com/graphql/v2"


def _connector_token(ctx: Context) -> str:
    req = ctx.request_context.request
    token = req.scope.get("mcp_token") if req is not None else None
    if not token:
        raise RuntimeError("Нет токена коннектора")
    return token


def _rw_errors(data: dict) -> str:
    msg = "; ".join(str(x.get("message", "?")) for x in data.get("errors", []))[:800]
    if "not authorized" in msg.lower():
        msg += (" (токен не подходит: нужен токен аккаунта или workspace из Railway → Account Settings → Tokens, "
                "и у него должен быть доступ к этим проектам)")
    return msg


async def rw(ctx: Context, account: str, query: str, variables: Optional[dict] = None, *,
             write: bool = False) -> dict:
    """Запрос к Railway GraphQL API от имени пользователя коннектора."""
    rw_token, readonly, label = await run_in_threadpool(_resolve_account, _connector_token(ctx), "railway", account)
    if write and readonly:
        raise RuntimeError(f"Эта ссылка даёт аккаунту «{label}» только чтение. Права меняются на сайте: «Настроить»")
    r = await http().post(RAILWAY_URL, json={"query": query, "variables": variables or {}}, headers={
        "Authorization": f"Bearer {rw_token}", "Content-Type": "application/json",
        "User-Agent": "github-connector-for-claude"})
    try:
        data = r.json()
    except ValueError:
        raise RuntimeError(f"Railway {r.status_code}: {r.text[:300]}")
    if data.get("errors"):
        raise RuntimeError("Railway: " + _rw_errors(data))
    if r.status_code >= 400:
        raise RuntimeError(f"Railway {r.status_code}: {r.text[:300]}")
    return data.get("data") or {}


def _nodes(conn) -> list:
    return [x["node"] for x in (conn or {}).get("edges", []) if x.get("node")]


@mcp.tool()
async def railway_projects(ctx: Context, workspace_id: str = "", account: str = "") -> list[dict]:
    """Railway: проекты с окружениями и сервисами (id и имена). С них начинается работа: id нужны остальным
    railway_* инструментам. workspace_id нужен, только если проекты лежат в workspace."""
    arg = ", workspaceId: $ws" if workspace_id else ""
    decl = "($ws: String)" if workspace_id else ""
    q = (f"query{decl} {{ projects(first: 50{arg}) {{ edges {{ node {{ id name "
         "environments { edges { node { id name } } } services { edges { node { id name } } } } } } }")
    data = await rw(ctx, account, q, {"ws": workspace_id} if workspace_id else None)
    return [{"id": p["id"], "name": p["name"],
             "environments": [{"id": x["id"], "name": x["name"]} for x in _nodes(p.get("environments"))],
             "services": [{"id": x["id"], "name": x["name"]} for x in _nodes(p.get("services"))]}
            for p in _nodes(data.get("projects"))]


@mcp.tool()
async def railway_deployments(ctx: Context, project_id: str, environment_id: str, service_id: str,
                              limit: int = 5, account: str = "") -> list[dict]:
    """Railway: последние деплои сервиса (id, статус, время). id деплоя нужен для railway_logs."""
    q = ("query($i: DeploymentListInput!, $n: Int) { deployments(input: $i, first: $n) "
         "{ edges { node { id status createdAt } } } }")
    data = await rw(ctx, account, q, {"i": {"projectId": project_id, "environmentId": environment_id,
                                   "serviceId": service_id}, "n": limit})
    return _nodes(data.get("deployments"))


@mcp.tool()
async def railway_logs(ctx: Context, deployment_id: str, kind: str = "deploy", limit: int = 100, account: str = "") -> str:
    """Railway: логи деплоя (последние строки). kind: deploy (рантайм) или build (сборка)."""
    field = "buildLogs" if kind == "build" else "deploymentLogs"
    q = f"query($id: String!, $n: Int) {{ {field}(deploymentId: $id, limit: $n) {{ message severity timestamp }} }}"
    data = await rw(ctx, account, q, {"id": deployment_id, "n": limit})
    lines = (data.get(field) or [])[-limit:]
    out = "\n".join(f"{(x.get('timestamp') or '')[:19]} {x.get('severity') or ''} {x.get('message') or ''}".strip()
                    for x in lines)
    return out[-30000:] or "(логов нет)"


@mcp.tool()
async def railway_variables(ctx: Context, project_id: str, environment_id: str, service_id: str = "",
                            show_values: bool = False, account: str = "") -> dict:
    """Railway: переменные окружения сервиса. По умолчанию только имена. show_values=true покажет значения,
    а они секретные и попадут в чат: используй, только если без них не обойтись."""
    q = ("query($p: String!, $e: String!, $s: String) "
         "{ variables(projectId: $p, environmentId: $e, serviceId: $s) }")
    data = await rw(ctx, account, q, {"p": project_id, "e": environment_id, "s": service_id or None})
    vs = data.get("variables") or {}
    if show_values:
        return vs
    return {"names": sorted(vs.keys()), "note": "значения скрыты; show_values=true покажет их"}


@mcp.tool()
async def railway_set_variable(ctx: Context, project_id: str, environment_id: str, name: str, value: str,
                               service_id: str = "", redeploy: bool = False, account: str = "") -> dict:
    """Railway: задать или изменить переменную окружения. redeploy=true сразу применит её новым деплоем."""
    inp: dict = {"projectId": project_id, "environmentId": environment_id, "name": name, "value": value,
                 "skipDeploys": not redeploy}
    if service_id:
        inp["serviceId"] = service_id
    data = await rw(ctx, account, "mutation($i: VariableUpsertInput!) { variableUpsert(input: $i) }", {"i": inp}, write=True)
    return {"ok": data.get("variableUpsert"), "name": name, "redeploy": redeploy}


@mcp.tool()
async def railway_redeploy(ctx: Context, service_id: str, environment_id: str, account: str = "") -> dict:
    """Railway: пересобрать и задеплоить сервис заново."""
    data = await rw(ctx, account, "mutation($s: String!, $e: String!) { serviceInstanceRedeploy(serviceId: $s, "
                    "environmentId: $e) }", {"s": service_id, "e": environment_id}, write=True)
    return {"ok": data.get("serviceInstanceRedeploy")}


@mcp.tool()
async def railway_restart(ctx: Context, deployment_id: str, account: str = "") -> dict:
    """Railway: перезапустить текущий деплой без пересборки."""
    data = await rw(ctx, account, "mutation($id: String!) { deploymentRestart(id: $id) }", {"id": deployment_id}, write=True)
    return {"ok": data.get("deploymentRestart")}


@mcp.tool()
async def railway_graphql(ctx: Context, query: str, variables: Optional[dict] = None, account: str = ""):
    """Railway: любой GraphQL-запрос к их API (для всего, чего нет в готовых railway_* инструментах).
    mutation разрешены только в режиме записи. Если не знаешь поля, сначала railway_schema."""
    head = re.sub(r"#.*", "", query).lstrip().lower()
    if head.startswith("subscription"):
        raise RuntimeError("subscription не поддерживаются")
    data = await rw(ctx, account, query, variables, write=head.startswith("mutation"))
    text = json.dumps(data, ensure_ascii=False)
    return data if len(text) < 40000 else text[:40000] + "…(обрезано)"


def _gql_type(t) -> str:
    if not t:
        return "?"
    if t.get("kind") == "NON_NULL":
        return _gql_type(t.get("ofType")) + "!"
    if t.get("kind") == "LIST":
        return "[" + _gql_type(t.get("ofType")) + "]"
    return t.get("name") or "?"


@mcp.tool()
async def railway_schema(ctx: Context, type_name: str = "Query", search: str = "", account: str = "") -> dict:
    """Railway: посмотреть живую схему API. type_name: Query, Mutation или имя типа/input (например,
    Deployment, DeploymentListInput). search фильтрует поля по подстроке. Так можно сверить имена и аргументы."""
    tr = "kind name ofType { kind name ofType { kind name ofType { kind name } } }"
    q = ("query($n: String!) { __type(name: $n) { name kind "
         f"fields {{ name args {{ name type {{ {tr} }} }} type {{ {tr} }} }} "
         f"inputFields {{ name type {{ {tr} }} }} enumValues {{ name }} }} }}")
    t = (await rw(ctx, account, q, {"n": type_name})).get("__type")
    if not t:
        raise RuntimeError(f"Тип {type_name} не найден")
    s = search.lower()
    fields = [f for f in (t.get("fields") or []) if s in f["name"].lower()]
    return {
        "type": t["name"], "kind": t["kind"],
        "fields": [{"name": f["name"], "type": _gql_type(f["type"]),
                    "args": {a["name"]: _gql_type(a["type"]) for a in f.get("args") or []}} for f in fields[:80]],
        "inputFields": {f["name"]: _gql_type(f["type"]) for f in t.get("inputFields") or []},
        "enumValues": [v["name"] for v in t.get("enumValues") or []],
    }


mcp_app = mcp.streamable_http_app()


# ---------------------------------------------------------------- web
@asynccontextmanager
async def lifespan(_: FastAPI):
    async with mcp.session_manager.run():
        try:
            yield
        finally:
            if _client is not None and not _client.is_closed:
                await _client.aclose()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(
    SessionMiddleware, secret_key=SECRET_KEY, same_site="lax",
    https_only=BASE_URL.startswith("https"), max_age=30 * 24 * 3600,
)

CSS = """
*{box-sizing:border-box}body{margin:0;font:16px/1.5 system-ui,sans-serif;background:#0f1115;color:#e6e8ec}
main{max-width:720px;margin:0 auto;padding:32px 20px}h1{font-size:28px;margin:0 0 8px}h2{font-size:18px;margin:28px 0 10px}
.card{background:#171a21;border:1px solid #262b36;border-radius:12px;padding:18px;margin:12px 0}
.muted{color:#8b93a4;font-size:14px}.btn,button{background:#d97757;color:#fff;border:0;border-radius:8px;padding:10px 16px;
font-size:15px;cursor:pointer;text-decoration:none;display:inline-block}button.ghost,.btn.ghost{background:#262b36}
input[type=text],input[type=password],select{background:#0f1115;color:#e6e8ec;border:1px solid #333a49;border-radius:8px;padding:10px;width:100%;font-size:15px}
table{width:100%;border-collapse:collapse;font-size:14px}td,th{text-align:left;padding:8px 6px;border-bottom:1px solid #262b36}
.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.ok{color:#5ec27a}.warn{background:#2a2414;border-color:#5a4a1a}
nav{display:flex;justify-content:space-between;align-items:center;margin-bottom:24px}code{word-break:break-all}
"""


def e(x) -> str:
    return html.escape(str(x if x is not None else ""))


def page(title: str, body: str, nav: str = "") -> HTMLResponse:
    return HTMLResponse(
        f"<!doctype html><html lang=ru><head><meta charset=utf-8>"
        f"<meta name=viewport content='width=device-width,initial-scale=1'><title>{e(title)}</title>"
        f"<style>{CSS}</style></head><body><main>{nav}{body}</main></body></html>"
    )


def get_user(request: Request, s: Session) -> Optional[User]:
    uid = request.session.get("uid")
    return s.get(User, uid) if uid else None


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    if request.session.get("uid"):
        return RedirectResponse("/dashboard")
    return page("GitHub для Claude", """
<h1>GitHub для Claude</h1>
<p class=muted>Подключи свой GitHub к Claude за минуту: войди, подключи GitHub, получи ссылку и вставь её в Claude.</p>
<div class=card><a class=btn href="/login/google">Войти через Google</a></div>
<ol class=muted><li>Войди через Google</li><li>Подключи GitHub</li><li>Создай ссылку коннектора</li>
<li>Claude → Settings → Connectors → Add custom connector → вставь ссылку</li></ol>""")


@app.get("/login/google")
def login_google(request: Request):
    state = secrets.token_urlsafe(16)
    request.session["gstate"] = state
    q = urlencode({
        "client_id": GOOGLE_CLIENT_ID, "redirect_uri": f"{BASE_URL}/auth/google/callback",
        "response_type": "code", "scope": "openid email profile", "state": state,
    })
    return RedirectResponse(f"https://accounts.google.com/o/oauth2/v2/auth?{q}")


@app.get("/auth/google/callback")
def google_callback(request: Request, code: str = "", state: str = ""):
    if not code or not state or state != request.session.pop("gstate", None):
        return page("Ошибка", "<p>Неверный state или вход отменён. <a href='/'>Назад</a></p>")
    tok = httpx.post("https://oauth2.googleapis.com/token", data={
        "code": code, "client_id": GOOGLE_CLIENT_ID, "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": f"{BASE_URL}/auth/google/callback", "grant_type": "authorization_code",
    }, timeout=20)
    if tok.status_code != 200:
        return page("Ошибка", "<p>Google отказал в обмене кода. <a href='/'>Назад</a></p>")
    info = httpx.get("https://openidconnect.googleapis.com/v1/userinfo",
                     headers={"Authorization": f"Bearer {tok.json()['access_token']}"}, timeout=20).json()
    email = (info.get("email") or "").lower()
    if not info.get("email_verified") or not email:
        return page("Ошибка", "<p>Email не подтверждён. <a href='/'>Назад</a></p>")
    if ALLOWED_EMAILS and email not in ALLOWED_EMAILS:
        return page("Нет доступа", "<p>Этот аккаунт не в списке разрешённых.</p>")
    with Session(engine) as s:
        u = s.scalar(select(User).where(User.google_sub == info["sub"]))
        if not u:
            u = User(google_sub=info["sub"], email=email, name=info.get("name"))
            s.add(u)
        else:
            u.email, u.name = email, info.get("name")
        s.commit()
        request.session["uid"] = u.id
    return RedirectResponse("/dashboard")


def _acc_table(accs: list) -> str:
    rows = ""
    for a in accs:
        mark = " <span class=ok>(основной)</span>" if a.is_default else ""
        mk = "" if a.is_default else (f"<form method=post action=/accounts/{a.id}/default>"
                                      f"<button class=ghost>Сделать основным</button></form>")
        rows += (f"<tr><td><b>{e(a.label)}</b>{mark}<br><span class=muted>{e(a.login)}</span></td>"
                 f"<td><div class=row>{mk}<form method=post action=/accounts/{a.id}/remove>"
                 f"<button class=ghost>Удалить</button></form></div></td></tr>")
    return f"<table>{rows}</table><br>" if rows else ""


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    with Session(engine) as s:
        u = get_user(request, s)
        if not u:
            return RedirectResponse("/")
        conns = s.scalars(select(Connector).where(Connector.user_id == u.id).order_by(Connector.id.desc())).all()
        accs = s.scalars(select(Account).where(Account.user_id == u.id).order_by(Account.id)).all()
        gh_accs = [x for x in accs if x.kind == "github"]
        rw_accs = [x for x in accs if x.kind == "railway"]
        nav = (f"<nav><b>GitHub для Claude</b><form method=post action=/logout class=row>"
               f"<span class=muted>{e(u.email)}</span><button class=ghost>Выйти</button></form></nav>")
        body = ""
        flash = request.session.pop("flash_url", None)
        if flash:
            body += (f"<div class='card warn'><b>Ссылка создана.</b> Скопируй сейчас, потом она не покажется.<br><br>"
                     f"<input type=text id=u value='{e(flash)}' readonly onclick='this.select()'><br><br>"
                     f"<button onclick=\"navigator.clipboard.writeText(document.getElementById('u').value);"
                     f"this.textContent='Скопировано'\">Копировать</button>"
                     f"<p class=muted>Claude → Settings → Connectors → Add custom connector → вставь ссылку.</p></div>")
        err = request.session.pop("flash_err", None)
        if err:
            body += f"<div class='card warn'><b>Не получилось.</b> {e(err)}</div>"
        body += ("<h2>1. Аккаунты GitHub</h2><div class=card>" + _acc_table(gh_accs) +
                 "<form method=get action=/github/connect><p class=muted>Добавить через вход в GitHub "
                 "(будет аккаунт, в который ты сейчас вошёл на github.com):</p>"
                 "<select name=scope><option value=private>Все репозитории, включая приватные</option>"
                 "<option value=public>Только публичные</option></select><br><br>"
                 "<button>Добавить через GitHub</button></form><hr style='border-color:#262b36'>"
                 "<form method=post action=/accounts/github-token autocomplete=off>"
                 "<p class=muted>Или по токену (удобно для другого аккаунта): github.com → Settings → Developer settings → "
                 "Personal access tokens, права <code>repo</code>. Если метка уже есть, токен заменится.</p>"
                 "<input type=text name=label placeholder='Метка (например, work), необязательно' maxlength=50><br><br>"
                 "<input type=password name=token placeholder='GitHub token' autocomplete=off required><br><br>"
                 "<button>Добавить по токену</button></form></div>")
        body += ("<h2>2. Аккаунты Railway <span class=muted>(необязательно)</span></h2><div class=card>" +
                 _acc_table(rw_accs) +
                 "<form method=post action=/railway/save autocomplete=off>"
                 "<p class=muted>Railway → Account Settings → Tokens → Create token (токен аккаунта или workspace). "
                 "Для другого аккаунта создай токен, войдя в него. Токен хранится зашифрованным и не показывается.</p>"
                 "<input type=text name=label placeholder='Метка (например, trial2), необязательно' maxlength=50><br><br>"
                 "<input type=password name=token placeholder='Railway API token' autocomplete=off required><br><br>"
                 "<button>Добавить Railway</button></form></div>")
        body += "<h2>3. Ссылки коннектора</h2>"
        if accs:
            body += ("<div class=card><form method=post action=/connectors/create>"
                     "<input type=text name=name placeholder='Название (например, Claude)' maxlength=100 required><br><br>"
                     "<p class=muted>Что разрешено этой ссылке по каждому аккаунту:</p>" + _perm_selects(accs, None) +
                     _tool_boxes(None) + "<br><button>Создать ссылку</button></form>"
                     "<p class=muted>Аккаунты, добавленные позже, в уже созданные ссылки сами не попадают: "
                     "включи их через «Настроить».</p></div>")
        else:
            body += "<p class=muted>Сначала добавь хотя бы один аккаунт.</p>"
        if conns:
            by_id = {a.id: a for a in accs}
            rows = ""
            for c in conns:
                p = _perms(c)
                if p is None:
                    acc_txt = "все аккаунты, " + ("только чтение" if c.readonly else "чтение и запись")
                else:
                    parts = [f"{e(by_id[i].label)}: {'чтение и запись' if m == 'w' else 'только чтение'}"
                             for i, m in p.items() if i in by_id and m in ("r", "w")]
                    acc_txt = "<br>".join(parts) or "нет доступа"
                last = c.last_used.strftime("%d.%m %H:%M") if c.last_used else "—"
                if c.revoked:
                    status = " <span class=muted>(отозвана)</span>"
                    btns = (f"<form method=post action=/connectors/{c.id}/delete>"
                            f"<button class=ghost>Удалить</button></form>")
                else:
                    status = ""
                    btns = (f"<a class='btn ghost' href=/connectors/{c.id}/edit>Настроить</a>"
                            f"<form method=post action=/connectors/{c.id}/revoke>"
                            f"<button class=ghost>Отозвать</button></form>")
                rows += (f"<tr><td>#{c.id}</td><td><b>{e(c.name)}</b>{status}<br><span class=muted>{acc_txt}</span></td>"
                         f"<td>{last}</td><td><div class=row>{btns}</div></td></tr>")
            clean = ""
            if any(c.revoked for c in conns):
                clean = ("<form method=post action=/connectors/delete-revoked>"
                         "<button class=ghost>Удалить все отозванные</button></form><br>")
            body += (f"<div class=card>{clean}<table><tr><th>ID</th><th>Ссылка и доступ</th><th>Исп.</th><th></th></tr>"
                     f"{rows}</table></div>")
        return page("Панель", body, nav)


def _save_account(s: Session, u: User, kind: str, label: str, login: str, tok: str) -> None:
    """Добавляет аккаунт или обновляет токен у существующего (совпала метка; для OAuth ещё и логин)."""
    label = (label or "").strip()[:50]
    key = label or login or kind
    conds = [func.lower(Account.label) == key.lower()]
    if not label and login:
        conds.append(func.lower(Account.login) == login.lower())
    existing = s.scalar(select(Account).where(Account.user_id == u.id, Account.kind == kind, or_(*conds)))
    enc = fernet.encrypt(tok.encode()).decode()
    if existing:
        existing.token_enc, existing.login = enc, login
    else:
        first = s.scalar(select(Account.id).where(Account.user_id == u.id, Account.kind == kind)) is None
        s.add(Account(user_id=u.id, kind=kind, label=key, login=login, token_enc=enc, is_default=first))
    s.commit()
    invalidate_cache()


@app.get("/github/connect")
def github_connect(request: Request, scope: str = "private"):
    if not request.session.get("uid"):
        return RedirectResponse("/")
    state = secrets.token_urlsafe(16)
    request.session["ghstate"] = state
    q = urlencode({
        "client_id": GITHUB_CLIENT_ID, "redirect_uri": f"{BASE_URL}/auth/github/callback",
        "scope": "repo" if scope == "private" else "public_repo", "state": state,
    })
    return RedirectResponse(f"https://github.com/login/oauth/authorize?{q}")


@app.get("/auth/github/callback")
def github_callback(request: Request, code: str = "", state: str = ""):
    if not request.session.get("uid"):
        return RedirectResponse("/")
    if not code or not state or state != request.session.pop("ghstate", None):
        return page("Ошибка", "<p>Неверный state или доступ отклонён. <a href='/dashboard'>Назад</a></p>")
    r = httpx.post("https://github.com/login/oauth/access_token", headers={"Accept": "application/json"}, data={
        "client_id": GITHUB_CLIENT_ID, "client_secret": GITHUB_CLIENT_SECRET,
        "code": code, "redirect_uri": f"{BASE_URL}/auth/github/callback",
    }, timeout=20).json()
    token = r.get("access_token")
    if not token:
        return page("Ошибка", f"<p>GitHub: {e(r.get('error_description', 'не выдал токен'))}. <a href='/dashboard'>Назад</a></p>")
    me = httpx.get("https://api.github.com/user", headers={"Authorization": f"Bearer {token}"}, timeout=20).json()
    with Session(engine) as s:
        u = get_user(request, s)
        if u and me.get("login"):
            _save_account(s, u, "github", "", me["login"], token)
    return RedirectResponse("/dashboard")


@app.post("/accounts/github-token")
def github_token_save(request: Request, token: str = Form(...), label: str = Form("")):
    tok = token.strip()
    with Session(engine) as s:
        u = get_user(request, s)
        if not u:
            return RedirectResponse("/", status_code=303)
        try:
            r = httpx.get("https://api.github.com/user", timeout=20, headers={
                "Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json",
                "User-Agent": "github-connector-for-claude"})
            login = r.json().get("login") if r.status_code == 200 else None
        except Exception as ex:
            request.session["flash_err"] = f"Не удалось связаться с GitHub: {type(ex).__name__}"
            return RedirectResponse("/dashboard", status_code=303)
        if not login:
            request.session["flash_err"] = "GitHub не принял токен (проверь, что он скопирован целиком и не истёк)"
            return RedirectResponse("/dashboard", status_code=303)
        _save_account(s, u, "github", label, login, tok)
    return RedirectResponse("/dashboard", status_code=303)


def _railway_probe(tok: str) -> tuple[bool, str]:
    """Проверяет токен Railway: (True, имя) или (False, причина). Токен аккаунта видит `me`, workspace-токен нет."""
    def call(q: str) -> dict:
        r = httpx.post(RAILWAY_URL, json={"query": q}, timeout=20, headers={
            "Authorization": f"Bearer {tok}", "Content-Type": "application/json",
            "User-Agent": "github-connector-for-claude"})
        return r.json()

    try:
        d = call("query { me { name email } }")
        me = (d.get("data") or {}).get("me")
        if me:
            return True, me.get("name") or me.get("email") or "аккаунт Railway"
        d2 = call("query { projects(first: 1) { edges { node { id } } } }")
        if (d2.get("data") or {}).get("projects") is not None:
            return True, "workspace-токен"
        return False, _rw_errors(d2) or _rw_errors(d) or "Railway не принял токен"
    except Exception as ex:  # сеть, не-JSON ответ
        return False, f"Не удалось связаться с Railway: {type(ex).__name__}"


@app.post("/railway/save")
def railway_save(request: Request, token: str = Form(...), label: str = Form("")):
    tok = token.strip()
    with Session(engine) as s:
        u = get_user(request, s)
        if not u:
            return RedirectResponse("/", status_code=303)
        ok, info = _railway_probe(tok)
        if not ok:
            request.session["flash_err"] = f"Railway не принял токен: {info}"
            return RedirectResponse("/dashboard", status_code=303)
        _save_account(s, u, "railway", label, info, tok)
    return RedirectResponse("/dashboard", status_code=303)


@app.post("/accounts/{aid}/remove")
def account_remove(request: Request, aid: int):
    with Session(engine) as s:
        u = get_user(request, s)
        a = s.get(Account, aid)
        if u and a and a.user_id == u.id:
            kind, was_default = a.kind, a.is_default
            s.delete(a)
            s.flush()
            if was_default:  # основным становится самый старый из оставшихся
                nxt = s.scalar(select(Account).where(Account.user_id == u.id, Account.kind == kind)
                               .order_by(Account.id))
                if nxt:
                    nxt.is_default = True
            s.commit()
    invalidate_cache()
    return RedirectResponse("/dashboard", status_code=303)


@app.post("/accounts/{aid}/default")
def account_default(request: Request, aid: int):
    with Session(engine) as s:
        u = get_user(request, s)
        a = s.get(Account, aid)
        if u and a and a.user_id == u.id:
            for x in s.scalars(select(Account).where(Account.user_id == u.id, Account.kind == a.kind)):
                x.is_default = x.id == a.id
            s.commit()
    invalidate_cache()
    return RedirectResponse("/dashboard", status_code=303)


try:
    import toolsets as TS
except Exception:  # noqa: BLE001  (без модуля ссылки работают как раньше: все инструменты)
    TS = None


def _tool_boxes(raw: Optional[str]) -> str:
    return TS.html_boxes(raw) if TS else ""


def _form_toolsets(form) -> Optional[str]:
    return TS.from_form(form) if TS else None


def _perm_selects(accs: list, current: Optional[dict]) -> str:
    """Таблица «аккаунт -> доступ». current None = для новой ссылки по умолчанию чтение и запись."""
    rows = ""
    for a in accs:
        cur = "w" if current is None else current.get(a.id, "")
        opts = "".join(f"<option value='{v}'{' selected' if cur == v else ''}>{t}</option>"
                       for v, t in (("", "Нет доступа"), ("r", "Только чтение"), ("w", "Чтение и запись")))
        rows += (f"<tr><td><b>{e(a.label)}</b><br><span class=muted>{e(_KIND_RU[a.kind])} · {e(a.login)}</span></td>"
                 f"<td><select name='acc_{a.id}'>{opts}</select></td></tr>")
    return f"<table>{rows}</table>"


def _form_perms(form, accs: list) -> dict:
    out = {}
    for a in accs:
        v = str(form.get(f"acc_{a.id}", ""))
        if v in ("r", "w"):
            out[str(a.id)] = v
    return out


@app.post("/connectors/create")
async def connector_create(request: Request):
    form = await request.form()
    name = str(form.get("name", "")).strip()[:100] or "Claude"

    def work():
        with Session(engine) as s:
            u = get_user(request, s)
            if not u:
                return None, None
            accs = s.scalars(select(Account).where(Account.user_id == u.id)).all()
            perms = _form_perms(form, accs)
            if not perms:
                return None, "Выбери доступ хотя бы к одному аккаунту"
            token = secrets.token_urlsafe(32)
            s.add(Connector(user_id=u.id, name=name, token_hash=sha(token),
                            readonly=all(v == "r" for v in perms.values()), perms=json.dumps(perms),
                            toolsets=_form_toolsets(form)))
            s.commit()
            return token, None

    token, err = await run_in_threadpool(work)
    if err:
        request.session["flash_err"] = err
    elif token:
        request.session["flash_url"] = f"{BASE_URL}/c/{token}/mcp"
    return RedirectResponse("/dashboard", status_code=303)


@app.get("/connectors/{cid}/edit", response_class=HTMLResponse)
def connector_edit_form(request: Request, cid: int):
    with Session(engine) as s:
        u = get_user(request, s)
        if not u:
            return RedirectResponse("/")
        c = s.get(Connector, cid)
        if not c or c.user_id != u.id or c.revoked:
            return RedirectResponse("/dashboard", status_code=303)
        accs = s.scalars(select(Account).where(Account.user_id == u.id).order_by(Account.id)).all()
        cur = _perms(c)
        if cur is None:  # старая ссылка «все аккаунты»: показываем как явные права
            cur = {a.id: ("r" if c.readonly else "w") for a in accs}
        err = request.session.pop("flash_err", None)
        body = (f"<nav><b>Настройка ссылки #{c.id}</b><a class='btn ghost' href=/dashboard>Назад</a></nav>"
                + (f"<div class='card warn'><b>Не получилось.</b> {e(err)}</div>" if err else "") +
                f"<div class=card><form method=post action=/connectors/{c.id}/edit>"
                f"<input type=text name=name value='{e(c.name)}' maxlength=100 required><br><br>"
                "<p class=muted>Что разрешено этой ссылке по каждому аккаунту:</p>" + _perm_selects(accs, cur) +
                _tool_boxes(c.toolsets) + "<br><button>Сохранить</button></form>"
                "<p class=muted>Сама ссылка не меняется, права действуют сразу. Чтобы выдать другую ссылку, "
                "создай новую и отзови эту.</p></div>")
        return page("Настройка ссылки", body)


@app.post("/connectors/{cid}/edit")
async def connector_edit(request: Request, cid: int):
    form = await request.form()

    def work():
        with Session(engine) as s:
            u = get_user(request, s)
            c = s.get(Connector, cid)
            if not u or not c or c.user_id != u.id or c.revoked:
                return None
            accs = s.scalars(select(Account).where(Account.user_id == u.id)).all()
            perms = _form_perms(form, accs)
            if not perms:
                return "Выбери доступ хотя бы к одному аккаунту (или отзови ссылку)"
            c.name = str(form.get("name", "")).strip()[:100] or c.name
            c.perms = json.dumps(perms)
            c.toolsets = _form_toolsets(form)
            c.readonly = all(v == "r" for v in perms.values())
            s.commit()
            return ""

    err = await run_in_threadpool(work)
    invalidate_cache()
    if err:
        request.session["flash_err"] = err
        return RedirectResponse(f"/connectors/{cid}/edit", status_code=303)
    return RedirectResponse("/dashboard", status_code=303)


@app.post("/connectors/delete-revoked")
def connector_delete_revoked(request: Request):
    with Session(engine) as s:
        u = get_user(request, s)
        if u:
            for c in s.scalars(select(Connector).where(Connector.user_id == u.id, Connector.revoked.is_(True))):
                s.delete(c)
            s.commit()
    return RedirectResponse("/dashboard", status_code=303)


@app.post("/connectors/{cid}/delete")
def connector_delete(request: Request, cid: int):
    with Session(engine) as s:
        u = get_user(request, s)
        c = s.get(Connector, cid)
        if u and c and c.user_id == u.id and c.revoked:  # удалить можно только уже отозванную
            s.delete(c)
            s.commit()
    return RedirectResponse("/dashboard", status_code=303)


@app.post("/connectors/{cid}/revoke")
def connector_revoke(request: Request, cid: int):
    with Session(engine) as s:
        u = get_user(request, s)
        c = s.get(Connector, cid)
        if u and c and c.user_id == u.id:
            c.revoked = True
            s.commit()
    invalidate_cache()
    return RedirectResponse("/dashboard", status_code=303)


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=303)


# ---------------------------------------------------------------- ASGI entrypoint
class McpRouter:
    """/c/<token>/mcp -> MCP-приложение; токен передаётся в scope, всё остальное идёт на сайт."""

    def __init__(self, site, mcp_asgi):
        self.site, self.mcp = site, mcp_asgi

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"].startswith("/c/"):
            parts = scope["path"].rstrip("/").split("/")  # ['', 'c', token, 'mcp']
            if len(parts) == 4 and parts[3] == "mcp":
                scope = dict(scope)
                scope["mcp_token"] = parts[2]
                scope["path"] = "/mcp"
                scope["raw_path"] = b"/mcp"
                return await self.mcp(scope, receive, send)
        await self.site(scope, receive, send)


application = McpRouter(app, mcp_app)
