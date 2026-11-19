"""Группы инструментов и метки (annotations). Чистые данные: этот модуль ничего не импортирует из app."""
import json
from typing import Optional

# ключ -> (название, пояснение, инструменты)
GROUPS: dict = {
    "gh_read": ("GitHub: чтение", "репозитории, файлы, коммиты, ветки, issues, PR, поиск, дерево, сравнение, история", [
        "list_repos", "search_repos", "search_code", "list_dir", "get_file", "list_commits", "list_branches",
        "list_issues", "get_issue", "list_pulls", "get_pull", "repo_overview", "repository_tree",
        "batch_read_files", "compare_branches", "commit_history_diff", "download_repository"]),
    "gh_write": ("GitHub: запись", "коммиты, правки, загрузка файлов, откат, ветки, issues, PR", [
        "put_file", "delete_file", "create_branch", "create_issue", "comment_issue", "create_pull", "merge_pull",
        "batch_commit", "upload_binary", "apply_edits", "rollback_commit"]),
    "actions_read": ("Actions и релизы: чтение", "запуски, джобы, логи, статус сборок, артефакты, релизы", [
        "list_workflow_runs", "get_run_jobs", "get_job_logs", "workflow_status", "list_artifacts",
        "download_artifact", "list_releases"]),
    "actions_write": ("Actions и релизы: запись", "запуск workflow, создание релизов, загрузка файлов в релиз", [
        "dispatch_workflow", "create_release", "upload_release_asset", "release_from_artifact"]),
    "rw_read": ("Railway: чтение", "проекты, деплои, логи, имена переменных, статус сервиса", [
        "railway_projects", "railway_deployments", "railway_logs", "railway_variables", "railway_service_status"]),
    "rw_write": ("Railway: запись", "смена переменных, редеплой, перезапуск", [
        "railway_set_variable", "railway_redeploy", "railway_restart"]),
    "advanced": ("Продвинутые", "github_api и railway_graphql: любой запрос к API, в том числе на запись", [
        "github_api", "railway_graphql", "railway_schema"]),
    "danger": ("Удаления (с подтверждением)", "удалить сервис или проект Railway, ветку GitHub", [
        "github_delete_branch", "railway_delete_service", "railway_delete_project"]),
    "checks": ("Проверки после деплоя", "запросы к сайту или API: статус, текст, время ответа", [
        "run_post_deploy_tests"]),
}
ALWAYS = {"list_accounts"}  # видно всегда

READ_ONLY = set(ALWAYS) | {"railway_schema"}
for _k in ("gh_read", "actions_read", "rw_read", "checks"):
    READ_ONLY |= set(GROUPS[_k][2])
DESTRUCTIVE = {"delete_file", "merge_pull", "railway_set_variable", "github_api", "railway_graphql"} | set(GROUPS["danger"][2])
CLOSED_WORLD = set(ALWAYS)  # остальные ходят во внешние сервисы


def annotation_for(name: str) -> dict:
    ro = name in READ_ONLY
    d = {"readOnlyHint": ro, "openWorldHint": name not in CLOSED_WORLD}
    if not ro:
        d["destructiveHint"] = name in DESTRUCTIVE
    return d


def parse(raw: Optional[str]) -> Optional[list]:
    """None = все инструменты. Битое значение считаем «ничего, кроме list_accounts» (безопасная сторона)."""
    if raw is None:
        return None
    try:
        v = json.loads(raw)
        return [k for k in v if k in GROUPS] if isinstance(v, list) else []
    except ValueError:
        return []


def allowed_tools(raw: Optional[str]) -> Optional[set]:
    keys = parse(raw)
    if keys is None:
        return None
    out = set(ALWAYS)
    for k in keys:
        out |= set(GROUPS[k][2])
    return out


def from_form(form) -> Optional[str]:
    """Отмечены все группы -> None (то есть «все», включая будущие); иначе JSON-список."""
    keys = [k for k in GROUPS if form.get(f"ts_{k}")]
    return None if len(keys) == len(GROUPS) else json.dumps(keys)


def html_boxes(raw: Optional[str]) -> str:
    sel = parse(raw)
    rows = ""
    for k, (title, desc, tools) in GROUPS.items():
        chk = " checked" if sel is None or k in sel else ""
        rows += (f"<label class=muted style='display:block;margin:6px 0'><input type=checkbox name='ts_{k}' "
                 f"value=1{chk}> <b>{title}</b> ({len(tools)}): {desc}</label>")
    return ("<p class=muted>Какие инструменты видит Claude. Чем меньше, тем меньше расход токенов и путаницы "
            "(list_accounts виден всегда):</p>" + rows)
