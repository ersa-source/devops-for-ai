"""Сборки, артефакты, релизы и статус Railway. Тяжёлые файлы (APK, ZIP) в чат не передаются:
сервер перекладывает их между GitHub Actions и релизами сам, а Claude получает ссылки и сводки."""
import base64
import io
import mimetypes
import zipfile
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, rw, _nodes
from common import ghx, fetch_bytes, pmap, q, clean_path, UPLOADS

MAX_ASSET = 100 * 1024 * 1024


async def _job_log_tail(ctx, account, o, r, job_id, lines=60) -> str:
    try:
        resp = await ghx(ctx, account, "GET", f"/repos/{o}/{r}/actions/jobs/{job_id}/logs", raw=True)
        if resp.is_redirect:
            data = await fetch_bytes(resp.headers["location"], 8 * 1024 * 1024)
        else:
            data = resp.content
        txt = data.decode("utf-8", "replace").splitlines()
        # убираем временные метки в начале строк
        txt = [x.split(" ", 1)[1] if x[:4].isdigit() and " " in x else x for x in txt]
        return "\n".join(txt[-lines:])[-6000:]
    except RuntimeError as e:
        return f"(логи недоступны: {str(e)[:150]})"


@mcp.tool()
async def workflow_status(ctx: Context, owner: str, repo: str, branch: str = "", with_logs: bool = True,
                          account: str = "") -> dict:
    """Статус сборок одним вызовом: последние запуски Actions и, если новейший упал, какие джобы и шаги
    провалились плюс хвост лога каждой упавшей джобы."""
    params = {"per_page": 5}
    if branch:
        params["branch"] = branch
    runs = (await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/actions/runs", params=params)).get("workflow_runs", [])
    out: dict = {"runs": [{"id": x["id"], "workflow": x["name"], "branch": x["head_branch"],
                           "commit": x["head_sha"][:10], "status": x["status"], "conclusion": x["conclusion"],
                           "created": x["created_at"], "url": x["html_url"]} for x in runs]}
    if not runs:
        out["note"] = "Запусков нет"
        return out
    latest = runs[0]
    if latest["conclusion"] != "failure":
        out["verdict"] = f"новейший запуск: {latest['conclusion'] or latest['status']}"
        return out
    jobs = (await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/actions/runs/{latest['id']}/jobs")).get("jobs", [])
    failed = [j for j in jobs if j["conclusion"] == "failure"]
    out["verdict"] = "новейший запуск упал"

    async def one(j):
        item = {"job": j["name"], "id": j["id"],
                "failed_steps": [s["name"] for s in j.get("steps", []) if s.get("conclusion") == "failure"]}
        if with_logs:
            item["log_tail"] = await _job_log_tail(ctx, account, owner, repo, j["id"])
        return item

    out["failed_jobs"] = await pmap(one, failed[:4], limit=4)
    return out


# ---------- артефакты ----------

@mcp.tool()
async def list_artifacts(ctx: Context, owner: str, repo: str, run_id: int = 0, limit: int = 20,
                         account: str = "") -> list[dict]:
    """Артефакты Actions (например собранный APK). run_id: только из этого запуска; без него: последние по репозиторию."""
    path = f"/repos/{owner}/{repo}/actions/runs/{run_id}/artifacts" if run_id else f"/repos/{owner}/{repo}/actions/artifacts"
    data = await ghx(ctx, account, "GET", path, params={"per_page": min(limit, 100)})
    return [{"id": a["id"], "name": a["name"], "size_mb": round(a["size_in_bytes"] / 1048576, 2),
             "expired": a["expired"], "created": a["created_at"],
             "run_id": (a.get("workflow_run") or {}).get("id"), "branch": (a.get("workflow_run") or {}).get("head_branch")}
            for a in data.get("artifacts", [])]


@mcp.tool()
async def download_artifact(ctx: Context, owner: str, repo: str, artifact_id: int, list_contents: bool = True,
                            account: str = "") -> dict:
    """Артефакт Actions: временная ссылка на ZIP (живёт ~1 минуту, открывать сразу) и список файлов внутри.
    Сам файл в чат не передаётся. Чтобы выдать его постоянной ссылкой, используй release_from_artifact."""
    r = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/actions/artifacts/{artifact_id}/zip")
    url = r.get("redirect") if isinstance(r, dict) else None
    out: dict = {"zip_url": url}
    if list_contents and url:
        try:
            data = await fetch_bytes(url, MAX_ASSET)
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                out["files"] = [{"name": i.filename, "size": i.file_size} for i in z.infolist() if not i.is_dir()][:200]
        except Exception as e:
            out["contents_error"] = str(e)[:200]
    return out


# ---------- релизы ----------

@mcp.tool()
async def list_releases(ctx: Context, owner: str, repo: str, limit: int = 10, account: str = "") -> list[dict]:
    """Релизы репозитория с вложенными файлами и их прямыми ссылками."""
    rel = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/releases", params={"per_page": min(limit, 50)})
    return [{"id": x["id"], "tag": x["tag_name"], "name": x["name"], "draft": x["draft"],
             "prerelease": x["prerelease"], "published": x["published_at"], "url": x["html_url"],
             "assets": [{"id": a["id"], "name": a["name"], "size_mb": round(a["size"] / 1048576, 2),
                         "download": a["browser_download_url"]} for a in x.get("assets", [])]} for x in rel]


@mcp.tool()
async def create_release(ctx: Context, owner: str, repo: str, tag: str, name: str = "", body: str = "",
                         target: str = "", draft: bool = False, prerelease: bool = False,
                         account: str = "") -> dict:
    """Создать релиз (и тег, если его нет). target: ветка или коммит, от которого делать тег (по умолчанию основная ветка)."""
    payload = {"tag_name": tag, "name": name or tag, "body": body, "draft": draft, "prerelease": prerelease}
    if target:
        payload["target_commitish"] = target
    x = await ghx(ctx, account, "POST", f"/repos/{owner}/{repo}/releases", write=True, json=payload)
    return {"id": x["id"], "tag": x["tag_name"], "url": x["html_url"], "upload": "upload_release_asset(release_id=…)"}


async def _upload_asset(ctx, account, o, r, release_id, name, data) -> dict:
    ctype = mimetypes.guess_type(name)[0] or "application/octet-stream"
    if name.lower().endswith(".apk"):
        ctype = "application/vnd.android.package-archive"
    a = await ghx(ctx, account, "POST", f"/repos/{o}/{r}/releases/{release_id}/assets", write=True, base=UPLOADS,
                  params={"name": name}, headers={"Content-Type": ctype}, content=data)
    return {"name": a["name"], "size_mb": round(a["size"] / 1048576, 2), "download": a["browser_download_url"]}


@mcp.tool()
async def upload_release_asset(ctx: Context, owner: str, repo: str, release_id: int, name: str,
                               url: str = "", content_base64: str = "", account: str = "") -> dict:
    """Прикрепить файл (APK, ZIP, HMOD, что угодно до 100 МБ) к релизу. Источник: url (сервер скачает сам)
    или content_base64 для маленьких файлов."""
    if bool(url) == bool(content_base64):
        raise RuntimeError("Нужно ровно одно из двух: url или content_base64")
    name = clean_path(name)
    if "/" in name:
        raise RuntimeError("name должно быть просто именем файла")
    if url:
        if not url.startswith("https://"):
            raise RuntimeError("Разрешены только https-ссылки")
        data = await fetch_bytes(url, MAX_ASSET)
    else:
        data = base64.b64decode(content_base64)
        if len(data) > MAX_ASSET:
            raise RuntimeError("Файл больше 100 МБ")
    return await _upload_asset(ctx, account, owner, repo, release_id, name, data)


@mcp.tool()
async def release_from_artifact(ctx: Context, owner: str, repo: str, artifact_id: int, tag: str, name: str = "",
                                body: str = "", target: str = "", extract: bool = True, prerelease: bool = False,
                                account: str = "") -> dict:
    """Одним вызовом: артефакт сборки → релиз с постоянными ссылками на файлы. Сервер сам скачивает артефакт и
    загружает его в релиз. extract=true: файлы из ZIP загружаются по отдельности (например app-release.apk),
    false: загружается сам ZIP. Если релиз с таким тегом уже есть, файлы добавляются к нему."""
    r = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/actions/artifacts/{artifact_id}/zip")
    url = r.get("redirect") if isinstance(r, dict) else None
    if not url:
        raise RuntimeError("GitHub не отдал ссылку на артефакт (возможно, он просрочен)")
    zdata = await fetch_bytes(url, MAX_ASSET)
    try:
        rel = await ghx(ctx, account, "GET", f"/repos/{owner}/{repo}/releases/tags/{q(tag)}")
    except RuntimeError as e:
        if " 404" not in str(e):
            raise
        payload = {"tag_name": tag, "name": name or tag, "body": body, "prerelease": prerelease}
        if target:
            payload["target_commitish"] = target
        rel = await ghx(ctx, account, "POST", f"/repos/{owner}/{repo}/releases", write=True, json=payload)
    assets = []
    if extract:
        with zipfile.ZipFile(io.BytesIO(zdata)) as z:
            for i in z.infolist():
                if i.is_dir():
                    continue
                assets.append(await _upload_asset(ctx, account, owner, repo, rel["id"], i.filename.split("/")[-1], z.read(i)))
    else:
        assets.append(await _upload_asset(ctx, account, owner, repo, rel["id"], f"artifact-{artifact_id}.zip", zdata))
    return {"release": rel["html_url"], "tag": rel["tag_name"], "assets": assets}


# ---------- Railway ----------

@mcp.tool()
async def railway_service_status(ctx: Context, project_id: str, environment_id: str, service_id: str,
                                 log_lines: int = 25, account: str = "") -> dict:
    """Railway: состояние сервиса одним вызовом: последние деплои и их статусы, а для проблемного или
    самого свежего деплоя хвост логов (сборки, если она упала, иначе рантайма)."""
    qd = ("query($i: DeploymentListInput!, $n: Int) { deployments(input: $i, first: $n) "
          "{ edges { node { id status createdAt } } } }")
    data = await rw(ctx, account, qd, {"i": {"projectId": project_id, "environmentId": environment_id,
                                             "serviceId": service_id}, "n": 5})
    deps = _nodes(data.get("deployments"))
    out: dict = {"deployments": deps}
    if not deps:
        out["note"] = "Деплоев нет"
        return out
    top = deps[0]
    out["state"] = top["status"]
    field = "buildLogs" if top["status"] in ("FAILED", "BUILDING", "INITIALIZING") else "deploymentLogs"
    ql = f"query($id: String!, $n: Int) {{ {field}(deploymentId: $id, limit: $n) {{ message severity timestamp }} }}"
    try:
        d = await rw(ctx, account, ql, {"id": top["id"], "n": log_lines})
        rows = (d.get(field) or [])[-log_lines:]
        out["log_kind"] = "build" if field == "buildLogs" else "runtime"
        out["log_tail"] = "\n".join(f"{(x.get('timestamp') or '')[11:19]} {x.get('severity') or ''} {x.get('message') or ''}".strip()
                                    for x in rows)[-5000:]
    except RuntimeError as e:
        out["log_error"] = str(e)[:200]
    return out
