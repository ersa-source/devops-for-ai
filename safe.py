"""Проверки после деплоя и опасные операции с подтверждением (двухшаговые, с одноразовым кодом)."""
import asyncio
import hashlib
import ipaddress
import secrets
import socket
import time
from typing import Optional
from urllib.parse import urlsplit

import httpx
from mcp.server.fastmcp import Context

from app import mcp, rw, _connector_token
from common import ghx, q

# ---------- проверки после деплоя ----------

MAX_BODY = 512 * 1024


def _check_host(url: str) -> None:
    """Не даём ходить во внутренние сети (localhost, 10.x, 169.254.x, *.railway.internal): защита от SSRF."""
    u = urlsplit(url)
    if u.scheme not in ("http", "https") or not u.hostname:
        raise RuntimeError("Нужна ссылка вида https://домен")
    host = u.hostname
    if host.endswith(".internal") or host == "localhost":
        raise RuntimeError("Внутренние адреса запрещены")
    try:
        infos = socket.getaddrinfo(host, u.port or (443 if u.scheme == "https" else 80), proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        raise RuntimeError(f"Домен {host} не разрешается")
    for i in infos:
        ip = ipaddress.ip_address(i[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
            raise RuntimeError("Внутренние и приватные адреса запрещены")


async def _one_check(base: str, c: dict) -> dict:
    method = (c.get("method") or "GET").upper()
    path = c.get("path") or "/"
    url = base.rstrip("/") + (path if path.startswith("/") else "/" + path)
    want = c.get("expect_status", 200)
    name = c.get("name") or f"{method} {path}"
    res = {"check": name, "url": url}
    if method not in ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"):
        return {**res, "ok": False, "error": f"метод {method} не поддерживается"}
    try:
        await asyncio.to_thread(_check_host, url)
        t0 = time.monotonic()
        async with httpx.AsyncClient(timeout=min(float(c.get("timeout", 10)), 20), follow_redirects=False) as cl:
            async with cl.stream(method, url, headers=c.get("headers"), json=c.get("json")) as r:
                body = bytearray()
                async for ch in r.aiter_bytes():
                    body += ch
                    if len(body) > MAX_BODY:
                        break
                ms = round((time.monotonic() - t0) * 1000)
                text = bytes(body).decode("utf-8", "replace")
                problems = []
                ok_status = r.status_code in (want if isinstance(want, list) else [want])
                if not ok_status:
                    problems.append(f"статус {r.status_code}, ожидался {want}")
                if c.get("contains") and c["contains"] not in text:
                    problems.append(f"в ответе нет текста {c['contains']!r}")
                if c.get("max_ms") and ms > c["max_ms"]:
                    problems.append(f"медленно: {ms} мс > {c['max_ms']} мс")
                if c.get("json_has"):
                    try:
                        data = __import__("json").loads(text)
                        for k in c["json_has"]:
                            if not isinstance(data, dict) or k not in data:
                                problems.append(f"в JSON нет поля {k!r}")
                    except ValueError:
                        problems.append("ответ не JSON")
                out = {**res, "ok": not problems, "status": r.status_code, "ms": ms}
                if problems:
                    out["problems"] = problems
                    out["body_start"] = text[:200]
                return out
    except RuntimeError as e:
        return {**res, "ok": False, "error": str(e)}
    except httpx.HTTPError as e:
        return {**res, "ok": False, "error": f"{type(e).__name__}: {str(e)[:150]}"}


@mcp.tool()
async def run_post_deploy_tests(ctx: Context, base_url: str, checks: Optional[list[dict]] = None,
                                wait_seconds: int = 0) -> dict:
    """Проверить сайт или API после деплоя. Запросы идут с сервера коннектора. checks: список проверок
    [{name?, path, method?=GET, expect_status?=200 (число или список), contains?, json_has?:[поля], max_ms?, timeout?, headers?, json?}].
    Без checks проверяется «/» на статус 200. wait_seconds (до 120): подождать перед стартом, пока сервис поднимается.
    Внутренние и приватные адреса запрещены. Возвращает pass/fail по каждой проверке."""
    await asyncio.to_thread(_check_host, base_url)
    if wait_seconds:
        await asyncio.sleep(max(0, min(wait_seconds, 120)))
    checks = checks or [{"path": "/"}]
    if len(checks) > 30:
        raise RuntimeError("Не больше 30 проверок за раз")
    results = await asyncio.gather(*[_one_check(base_url, c) for c in checks])
    passed = sum(1 for r in results if r["ok"])
    return {"passed": passed, "failed": len(results) - passed, "all_ok": passed == len(results), "results": results}


# ---------- опасные операции с подтверждением ----------

_PENDING: dict = {}
TTL = 600


def _who(ctx) -> str:
    return hashlib.sha256(_connector_token(ctx).encode()).hexdigest()


def _issue(ctx, action: str, params: dict) -> str:
    now = time.time()
    for k in [k for k, v in _PENDING.items() if v["exp"] < now]:
        del _PENDING[k]
    code = secrets.token_hex(3).upper()
    _PENDING[code] = {"who": _who(ctx), "action": action, "params": params, "exp": now + TTL}
    return code


def _take(ctx, code: str, action: str, params: dict) -> None:
    p = _PENDING.get((code or "").strip().upper())
    if not p or p["exp"] < time.time():
        raise RuntimeError("Код подтверждения неверный или просрочен (действует 10 минут). Запроси план заново")
    if p["who"] != _who(ctx) or p["action"] != action or p["params"] != params:
        raise RuntimeError("Код выдан для другого действия или параметров. Запроси план заново")
    del _PENDING[code.strip().upper()]  # одноразовый


def _plan(code: str, what: str, details: dict) -> dict:
    return {"confirmation_required": True, "will_do": what, **details, "confirm_code": code,
            "next": f"Покажи это пользователю. Если он согласен, повтори вызов с confirm_code=\"{code}\" (действует 10 минут)"}


@mcp.tool()
async def railway_delete_service(ctx: Context, project_id: str, service_id: str, environment_id: str = "",
                                 confirm_code: str = "", account: str = "") -> dict:
    """Railway: удалить сервис (необратимо). Двухшаговая операция: без confirm_code вернёт план и код,
    с кодом (только после согласия пользователя) выполнит удаление."""
    params = {"project_id": project_id, "service_id": service_id, "environment_id": environment_id}
    if not confirm_code:
        d = await rw(ctx, account, "query($s: String!) { service(id: $s) { name } }", {"s": service_id})
        name = (d.get("service") or {}).get("name", "?")
        code = _issue(ctx, "railway_delete_service", params)
        return _plan(code, f"УДАЛИТЬ сервис «{name}» вместе с деплоями и его переменными, это необратимо",
                     {"service": name, "service_id": service_id})
    _take(ctx, confirm_code, "railway_delete_service", params)
    q_ = "mutation($i: String!, $e: String) { serviceDelete(id: $i, environmentId: $e) }"
    d = await rw(ctx, account, q_, {"i": service_id, "e": environment_id or None}, write=True)
    return {"deleted": bool(d.get("serviceDelete")), "service_id": service_id}


@mcp.tool()
async def railway_delete_project(ctx: Context, project_id: str, confirm_code: str = "", account: str = "") -> dict:
    """Railway: удалить проект целиком со всеми сервисами и базами (необратимо). Двухшаговая операция:
    без confirm_code вернёт план и код, с кодом (только после согласия пользователя) выполнит удаление."""
    params = {"project_id": project_id}
    if not confirm_code:
        d = await rw(ctx, account, "query($p: String!) { project(id: $p) { name services { edges { node { name } } } } }",
                     {"p": project_id})
        pr = d.get("project") or {}
        names = [e["node"]["name"] for e in (pr.get("services") or {}).get("edges", [])]
        code = _issue(ctx, "railway_delete_project", params)
        return _plan(code, f"УДАЛИТЬ проект «{pr.get('name', '?')}» целиком: сервисы {names}, включая базы данных. Необратимо",
                     {"project": pr.get("name"), "services": names})
    _take(ctx, confirm_code, "railway_delete_project", params)
    d = await rw(ctx, account, "mutation($p: String!) { projectDelete(id: $p) }", {"p": project_id}, write=True)
    return {"deleted": bool(d.get("projectDelete")), "project_id": project_id}


@mcp.tool()
async def github_delete_branch(ctx: Context, owner: str, repo: str, branch: str, confirm_code: str = "",
                               account: str = "") -> dict:
    """GitHub: удалить ветку. Основную ветку удалить нельзя. Двухшаговая операция: без confirm_code вернёт план
    (с числом коммитов, которых нет в основной ветке) и код, с кодом выполнит удаление."""
    params = {"owner": owner, "repo": repo, "branch": branch}
    default = (await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}"))["default_branch"]
    if branch == default:
        raise RuntimeError("Основную ветку удалять нельзя")
    if not confirm_code:
        c = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/compare/{q(default)}...{q(branch)}")
        code = _issue(ctx, "github_delete_branch", params)
        warn = f"в ветке {c['ahead_by']} коммит(ов), которых нет в {default}: они потеряются" if c["ahead_by"] else \
            f"всё из ветки уже есть в {default}"
        return _plan(code, f"удалить ветку «{branch}» в {owner}/{repo}", {"unmerged_commits": c["ahead_by"], "note": warn})
    _take(ctx, confirm_code, "github_delete_branch", params)
    await ghx(ctx, account, "DELETE", f"/repos/{owner}/{repo}/git/refs/heads/{q(branch)}", write=True)
    return {"deleted": True, "branch": branch}
