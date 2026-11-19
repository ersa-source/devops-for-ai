"""Расширенные Git-инструменты: массовые операции, патчи, откат, сравнение, скачивание проекта.
Всё через Git Data API: один коммит на любое число файлов, ветка двигается только вперёд (без force)."""
import base64
import json
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp
from common import (ghx, fetch_bytes, pmap, q, blob_sha, looks_binary, clean_path, unified)

MAX_FILE = 5 * 1024 * 1024          # один файл при чтении
MAX_UPLOAD = 25 * 1024 * 1024       # загрузка по ссылке / base64
RAW = "application/vnd.github.raw+json"


# ---------- общие функции ----------

async def _default_branch(ctx, account, o, r) -> str:
    return (await ghx(ctx, account, "GET", f"/repos/{o}/{r}"))["default_branch"]


async def _get_file(ctx, account, o, r, path, ref) -> tuple[Optional[bytes], Optional[str]]:
    """(содержимое, sha блоба) или (None, None), если файла нет. Работает и для файлов больше 1 МБ."""
    params = {"ref": ref} if ref else {}
    try:
        meta = await ghx(ctx, account, "GET", f"/repos/{o}/{r}/contents/{q(path)}", params=params)
    except RuntimeError as e:
        if " 404" in str(e):
            return None, None
        raise
    if isinstance(meta, list):
        raise RuntimeError(f"«{path}» это папка, а не файл")
    if meta.get("type") != "file":
        raise RuntimeError(f"«{path}» не обычный файл (тип {meta.get('type')})")
    if meta.get("encoding") == "base64" and meta.get("content"):
        return base64.b64decode(meta["content"]), meta["sha"]
    resp = await ghx(ctx, account, "GET", f"/repos/{o}/{r}/contents/{q(path)}", params=params, raw=True, accept=RAW)
    return resp.content, meta["sha"]


def _text(data: bytes) -> Optional[str]:
    if looks_binary(data):
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


async def _commit(ctx, account, o, r, branch, message, changes, dry_run, diff_chars=2500):
    """changes: [{path, data: bytes | None (None = удалить)}]. Возвращает отчёт; при dry_run ничего не пишет."""
    if not changes:
        raise RuntimeError("Нет изменений: список файлов пуст")
    seen = set()
    for c in changes:
        c["path"] = clean_path(c["path"])
        if c["path"] in seen:
            raise RuntimeError(f"Путь «{c['path']}» указан дважды")
        seen.add(c["path"])
    branch = branch or await _default_branch(ctx, account, o, r)
    ref = await ghx(ctx, account, "GET", f"/repos/{o}/{r}/git/ref/heads/{q(branch)}")
    head = ref["object"]["sha"]
    base_tree = (await ghx(ctx, account, "GET", f"/repos/{o}/{r}/git/commits/{head}"))["tree"]["sha"]

    async def current(c):
        return await _get_file(ctx, account, o, r, c["path"], head)

    cur = await pmap(current, changes)
    report, todo = [], []
    for c, (old, old_sha) in zip(changes, cur):
        d = c["data"]
        item = {"path": c["path"]}
        if d is None:
            if old is None:
                item["status"] = "skip: файла и так нет"
            else:
                item["status"] = "delete"
                todo.append(c)
                t = _text(old)
                if t is not None:
                    item["removed_lines"] = len(t.splitlines())
        elif old is None:
            item["status"] = "add"
            item["bytes"] = len(d)
            todo.append(c)
            t = _text(d)
            if t is not None:
                item["added_lines"] = len(t.splitlines())
                if dry_run:
                    item["preview"] = t[:diff_chars]
            else:
                item["binary"] = True
        elif blob_sha(d) == old_sha:
            item["status"] = "unchanged"
        else:
            item["status"] = "modify"
            item["bytes"] = len(d)
            todo.append(c)
            to, tn = _text(old), _text(d)
            if to is not None and tn is not None:
                diff, add, rem = unified(to, tn, c["path"], diff_chars if dry_run else 0)
                item["added_lines"], item["removed_lines"] = add, rem
                if dry_run:
                    item["diff"] = diff
            else:
                item["binary"] = True
        report.append(item)
    out = {"repo": f"{o}/{r}", "branch": branch, "base_commit": head, "files": report}
    if dry_run:
        out["dry_run"] = True
        out["note"] = "Ничего не записано. Повтори вызов с dry_run=false, чтобы создать коммит."
        return out
    if not todo:
        out["note"] = "Коммит не создан: все файлы уже в нужном состоянии"
        return out

    async def mk_blob(c):
        if c["data"] is None:
            return {"path": c["path"], "mode": "100644", "type": "blob", "sha": None}
        b = await ghx(ctx, account, "POST", f"/repos/{o}/{r}/git/blobs", write=True,
                      json={"content": base64.b64encode(c["data"]).decode(), "encoding": "base64"})
        return {"path": c["path"], "mode": "100644", "type": "blob", "sha": b["sha"]}

    entries = await pmap(mk_blob, todo)
    tree = await ghx(ctx, account, "POST", f"/repos/{o}/{r}/git/trees", write=True,
                     json={"base_tree": base_tree, "tree": entries})
    commit = await ghx(ctx, account, "POST", f"/repos/{o}/{r}/git/commits", write=True,
                       json={"message": message, "tree": tree["sha"], "parents": [head]})
    await ghx(ctx, account, "PATCH", f"/repos/{o}/{r}/git/refs/heads/{q(branch)}", write=True,
              json={"sha": commit["sha"], "force": False})
    out["commit"] = commit["sha"]
    out["url"] = commit.get("html_url") or f"https://github.com/{o}/{r}/commit/{commit['sha']}"
    return out


def _decode_item(f: dict) -> Optional[bytes]:
    if f.get("content_base64") is not None:
        try:
            data = base64.b64decode(f["content_base64"], validate=False)
        except Exception:
            raise RuntimeError(f"«{f.get('path')}»: content_base64 не является корректным base64")
    elif f.get("content") is not None:
        data = f["content"].encode("utf-8")
    else:
        raise RuntimeError(f"«{f.get('path')}»: нужен content (текст) или content_base64")
    if len(data) > MAX_UPLOAD:
        raise RuntimeError(f"«{f.get('path')}»: больше {MAX_UPLOAD // (1024 * 1024)} МБ")
    return data


# ---------- чтение ----------

@mcp.tool()
async def repository_tree(ctx: Context, owner: str, repo: str, ref: str = "", path: str = "",
                          max_entries: int = 1500, account: str = "") -> dict:
    """Всё дерево файлов репозитория одним вызовом (с размерами). path: показать только эту папку."""
    ref = ref or await _default_branch(ctx, account, owner, repo)
    t = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/git/trees/{q(ref)}", params={"recursive": "1"})
    prefix = path.strip("/")
    files = []
    for e in t.get("tree", []):
        if e["type"] != "blob":
            continue
        if prefix and not (e["path"] == prefix or e["path"].startswith(prefix + "/")):
            continue
        files.append({"path": e["path"], "size": e.get("size", 0)})
    total = len(files)
    return {"ref": ref, "total_files": total, "truncated_by_github": bool(t.get("truncated")),
            "shown": min(total, max_entries), "files": files[:max_entries]}


@mcp.tool()
async def batch_read_files(ctx: Context, owner: str, repo: str, paths: list[str], ref: str = "",
                           max_chars_per_file: int = 20000, account: str = "") -> dict:
    """Прочитать много файлов за один вызов (параллельно). Бинарные файлы не выводятся, только размер."""
    if len(paths) > 60:
        raise RuntimeError("Не больше 60 файлов за раз")

    async def one(p):
        try:
            data, sha = await _get_file(ctx, account, owner, repo, clean_path(p), ref)
        except RuntimeError as e:
            return {"path": p, "error": str(e)[:300]}
        if data is None:
            return {"path": p, "error": "не найден"}
        t = _text(data)
        if t is None:
            return {"path": p, "binary": True, "size": len(data), "sha": sha}
        cut = len(t) > max_chars_per_file
        return {"path": p, "size": len(data), "sha": sha, "truncated": cut, "content": t[:max_chars_per_file]}

    return {"files": await pmap(one, paths)}


# ---------- запись ----------

@mcp.tool()
async def batch_commit(ctx: Context, owner: str, repo: str, message: str, files: list[dict],
                       delete: Optional[list[str]] = None, branch: str = "", dry_run: bool = False,
                       account: str = "") -> dict:
    """Много файлов одним коммитом. files: [{path, content}] для текста или [{path, content_base64}] для любых файлов.
    delete: пути для удаления. dry_run=true покажет diff и ничего не запишет. Неизменённые файлы пропускаются."""
    changes = [{"path": f.get("path"), "data": _decode_item(f)} for f in files]
    changes += [{"path": p, "data": None} for p in (delete or [])]
    return await _commit(ctx, account, owner, repo, branch, message, changes, dry_run)


@mcp.tool()
async def upload_binary(ctx: Context, owner: str, repo: str, path: str, message: str, content_base64: str = "",
                        url: str = "", branch: str = "", account: str = "") -> dict:
    """Загрузить любой файл (картинка, ZIP, APK…) в репозиторий. Либо content_base64 (маленькие файлы),
    либо url: сервер сам скачает файл (до 25 МБ). Для файлов больше 25 МБ используй релизы."""
    if bool(content_base64) == bool(url):
        raise RuntimeError("Нужно ровно одно из двух: content_base64 или url")
    if url:
        if not url.startswith("https://"):
            raise RuntimeError("Разрешены только https-ссылки")
        data = await fetch_bytes(url, MAX_UPLOAD)
    else:
        data = _decode_item({"path": path, "content_base64": content_base64})
    res = await _commit(ctx, account, owner, repo, branch, message, [{"path": path, "data": data}], False)
    res["size"] = len(data)
    return res


@mcp.tool()
async def apply_edits(ctx: Context, owner: str, repo: str, message: str, files: list[dict],
                      branch: str = "", dry_run: bool = True, account: str = "") -> dict:
    """Точечные правки без пересылки файлов целиком. files: [{path, edits:[{old, new, replace_all?}], expected_sha?}].
    Фрагмент old должен встретиться в файле ровно один раз (или replace_all=true). При любом конфликте
    не записывается НИЧЕГО, а в ответе указано, какой фрагмент не найден или неоднозначен.
    expected_sha: git-sha файла, который ты читал; если файл с тех пор изменился, правка отклоняется.
    По умолчанию dry_run=true: сначала покажет diff, для записи поставь dry_run=false."""
    branch_name = branch or await _default_branch(ctx, account, owner, repo)

    async def one(f):
        path = clean_path(f.get("path"))
        data, sha = await _get_file(ctx, account, owner, repo, path, branch_name)
        if data is None:
            return {"path": path, "conflict": "файл не найден"}
        if f.get("expected_sha") and f["expected_sha"] != sha:
            return {"path": path, "conflict": f"файл изменился (сейчас sha {sha}, ожидался {f['expected_sha']}). "
                                              "Прочитай его заново"}
        text = _text(data)
        if text is None:
            return {"path": path, "conflict": "бинарный файл, правки не применимы"}
        problems = []
        for i, ed in enumerate(f.get("edits") or [], 1):
            old, new = ed.get("old"), ed.get("new", "")
            if not old:
                problems.append(f"правка {i}: пустой old")
                continue
            n = text.count(old)
            if n == 0:
                problems.append(f"правка {i}: фрагмент не найден: {old[:80]!r}")
            elif n > 1 and not ed.get("replace_all"):
                problems.append(f"правка {i}: фрагмент встречается {n} раз, добавь контекста или replace_all=true: {old[:80]!r}")
            else:
                text = text.replace(old, new) if ed.get("replace_all") else text.replace(old, new, 1)
        if not f.get("edits"):
            problems.append("нет правок")
        if problems:
            return {"path": path, "conflict": problems}
        return {"path": path, "data": text.encode("utf-8")}

    results = await pmap(one, files)
    bad = [{"path": r["path"], "conflict": r["conflict"]} for r in results if "conflict" in r]
    if bad:
        return {"applied": False, "note": "Конфликты, ничего не записано", "conflicts": bad}
    return await _commit(ctx, account, owner, repo, branch, message,
                         [{"path": r["path"], "data": r["data"]} for r in results], dry_run)


# ---------- история и сравнение ----------

@mcp.tool()
async def compare_branches(ctx: Context, owner: str, repo: str, base: str, head: str,
                           max_files: int = 60, account: str = "") -> dict:
    """Сравнить две ветки/коммита: насколько расходятся, какие коммиты и файлы отличаются."""
    c = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/compare/{q(base)}...{q(head)}", params={"per_page": 100})
    files = c.get("files", [])
    return {
        "status": c["status"], "ahead_by": c["ahead_by"], "behind_by": c["behind_by"],
        "total_commits": c["total_commits"], "url": c.get("html_url"),
        "commits": [{"sha": x["sha"][:10], "message": x["commit"]["message"].splitlines()[0][:100],
                     "author": x["commit"]["author"]["name"]} for x in c.get("commits", [])[-30:]],
        "files_changed": len(files),
        "files": [{"path": f["filename"], "status": f["status"], "+": f["additions"], "-": f["deletions"]}
                  for f in files[:max_files]],
    }


@mcp.tool()
async def commit_history_diff(ctx: Context, owner: str, repo: str, branch: str = "", path: str = "",
                              limit: int = 10, with_ci: bool = True, account: str = "") -> dict:
    """Последние коммиты с составом изменений и результатом CI по каждому: помогает найти, какая правка сломала проект.
    suspect = первый (самый новый) коммит, после которого CI стал красным, а до него был зелёным."""
    limit = max(1, min(limit, 25))
    params = {"per_page": limit}
    if branch:
        params["sha"] = branch
    if path:
        params["path"] = path
    commits = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/commits", params=params)

    async def one(c):
        sha = c["sha"]
        det = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/commits/{sha}")
        item = {"sha": sha[:10], "message": c["commit"]["message"].splitlines()[0][:100],
                "author": c["commit"]["author"]["name"], "date": c["commit"]["author"]["date"],
                "stats": det.get("stats"),
                "files": [f"{f['status'][0].upper()} {f['filename']}" for f in det.get("files", [])[:20]]}
        if with_ci:
            try:
                runs = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/actions/runs",
                                 params={"head_sha": sha, "per_page": 5})
                rs = runs.get("workflow_runs", [])
                concl = [x["conclusion"] or x["status"] for x in rs]
                item["ci"] = ("failure" if "failure" in concl else "in_progress" if any(
                    x not in ("success", "skipped", "neutral", "cancelled") for x in concl) else
                    "success" if concl else "none")
            except RuntimeError:
                item["ci"] = "unknown"
        return item

    items = await pmap(one, commits, limit=5)
    suspect = None
    if with_ci:
        for i, it in enumerate(items):  # от нового к старому
            if it.get("ci") == "failure":
                nxt = items[i + 1] if i + 1 < len(items) else None
                if nxt is None or nxt.get("ci") in ("success", "none"):
                    suspect = it["sha"]
                    break
    return {"commits": items, "suspect": suspect,
            "hint": "suspect: на этом коммите CI впервые краснеет; смотри его files" if suspect else None}


@mcp.tool()
async def rollback_commit(ctx: Context, owner: str, repo: str, commit_sha: str, branch: str = "",
                          dry_run: bool = True, account: str = "") -> dict:
    """Безопасный откат: создаёт НОВЫЙ коммит, отменяющий изменения указанного (история не переписывается).
    Если файл после этого коммита менялся ещё раз, он помечается как конфликт и откат не выполняется.
    По умолчанию dry_run=true: покажет, что будет откачено."""
    branch_name = branch or await _default_branch(ctx, account, owner, repo)
    c = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/commits/{commit_sha}")
    if len(c.get("parents", [])) != 1:
        raise RuntimeError("Откат поддерживает только обычные коммиты с одним родителем (не merge и не корневой)")
    parent = c["parents"][0]["sha"]
    files = c.get("files", [])
    if len(files) >= 300:
        raise RuntimeError("В коммите 300+ файлов, откат такого размера не поддерживается")

    async def one(f):
        path, st = f["filename"], f["status"]
        cur, cur_sha = await _get_file(ctx, account, owner, repo, path, branch_name)
        if st == "renamed":
            prev = f["previous_filename"]
            old, _ = await _get_file(ctx, account, owner, repo, prev, parent)
            if cur is not None and cur_sha != f["sha"]:
                return {"conflict": f"{path}: файл менялся после коммита"}
            return {"ops": [{"path": path, "data": None}, {"path": prev, "data": old}]}
        if st == "added":
            if cur is None:
                return {"ops": []}
            if cur_sha != f["sha"]:
                return {"conflict": f"{path}: файл менялся после коммита"}
            return {"ops": [{"path": path, "data": None}]}
        # modified / removed / changed
        old, _ = await _get_file(ctx, account, owner, repo, path, parent)
        if st == "removed":
            if cur is not None:
                return {"conflict": f"{path}: файл снова существует с другим содержимым"}
            return {"ops": [{"path": path, "data": old}]}
        if cur is None or cur_sha != f["sha"]:
            return {"conflict": f"{path}: файл менялся после коммита"}
        return {"ops": [{"path": path, "data": old}]}

    results = await pmap(one, files, limit=6)
    conflicts = [r["conflict"] for r in results if "conflict" in r]
    if conflicts:
        return {"rolled_back": False, "note": "Откат невозможен без ручного разбора: файлы менялись позже",
                "conflicts": conflicts}
    ops = [o for r in results for o in r["ops"]]
    msg = f"Revert «{c['commit']['message'].splitlines()[0][:80]}»\n\nОткат коммита {commit_sha}"
    return await _commit(ctx, account, owner, repo, branch_name, msg, ops, dry_run)


# ---------- скачивание проекта ----------

@mcp.tool()
async def download_repository(ctx: Context, owner: str, repo: str, ref: str = "", path: str = "",
                              max_files: int = 80, max_chars_per_file: int = 12000,
                              max_total_chars: int = 150000, account: str = "") -> dict:
    """Весь проект сразу. Возвращает (1) zip_url: временная ссылка на ZIP-архив (живёт около минуты, открывать
    сразу; для приватных репозиториев работает без токена, пока ссылка свежая) и (2) текстовый дамп файлов
    для чтения прямо в чате: бинарные пропускаются, большие обрезаются, общий лимит max_total_chars."""
    ref = ref or await _default_branch(ctx, account, owner, repo)
    out: dict = {"ref": ref}
    try:
        z = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/zipball/{q(ref)}")
        out["zip_url"] = z.get("redirect") if isinstance(z, dict) else None
    except RuntimeError as e:
        out["zip_error"] = str(e)[:200]
    t = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/git/trees/{q(ref)}", params={"recursive": "1"})
    prefix = path.strip("/")
    blobs = [e for e in t.get("tree", []) if e["type"] == "blob"
             and (not prefix or e["path"] == prefix or e["path"].startswith(prefix + "/"))]
    out["total_files"] = len(blobs)
    skipped, picks = [], []
    for e in sorted(blobs, key=lambda x: x["path"]):
        if e.get("size", 0) > MAX_FILE:
            skipped.append(f"{e['path']} (>{MAX_FILE // (1024 * 1024)} МБ)")
        elif len(picks) < max_files:
            picks.append(e)
        else:
            skipped.append(e["path"])

    async def rd(e):
        try:
            data, _ = await _get_file(ctx, account, owner, repo, e["path"], ref)
        except RuntimeError as ex:
            return {"path": e["path"], "error": str(ex)[:120]}
        if data is None:
            return {"path": e["path"], "error": "не найден"}
        tx = _text(data)
        if tx is None:
            return {"path": e["path"], "binary": True, "size": len(data)}
        return {"path": e["path"], "size": len(data), "content": tx[:max_chars_per_file],
                "truncated": len(tx) > max_chars_per_file}

    res = await pmap(rd, picks)
    used, final = 0, []
    for r in res:
        c = r.get("content")
        if c is not None:
            if used + len(c) > max_total_chars:
                skipped.append(f"{r['path']} (лимит общего объёма)")
                continue
            used += len(c)
        final.append(r)
    out["files"] = final
    out["not_included"] = skipped[:100]
    return out
