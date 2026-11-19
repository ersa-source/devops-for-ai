# DevOps for AI

**A personal MCP server that lets an AI assistant (Claude) work with your GitHub and Railway accounts: read code, make safe multi-file commits, check CI, roll back, deploy and verify.**

> **This is a personal project.** I built it for my own workflow and I'm sharing it as-is. It is not a product, it is not audited, and it is not affiliated with or endorsed by Anthropic, OpenAI, GitHub or Railway. Self-host it, read the code, and use it at your own risk.

It is one small FastAPI app that is both a **website** (sign in, add accounts, create connector links) and an **MCP server** (Streamable HTTP). You paste a connector link into Claude (Settings, Connectors, Add custom connector) and the assistant gets ~55 tools.

[Русская версия ниже](#русский)

## What it does

- **Multiple accounts.** Any number of GitHub and Railway accounts, each with a label. The assistant picks one with an `account` argument; calls to different accounts run in parallel.
- **Per-link permissions.** Every connector link has its own rights per account: none / read-only / read-write. Links can be revoked, edited and deleted.
- **Per-link tool sets.** Each link also chooses which groups of tools the assistant can even see (GitHub read, GitHub write, Actions, Railway read/write, advanced, deletions, checks). Fewer tools means fewer tokens and fewer wrong tool choices. Hidden tools are also blocked at call time.
- **Safe file operations** (Git Data API, one commit for any number of files, fast-forward only, never force):
  - `batch_commit` (text and binary files, deletions, `dry_run` shows diffs without writing)
  - `apply_edits` (targeted old-to-new edits; any conflict means nothing is written; optional `expected_sha` check)
  - `rollback_commit` (reverts a commit with a new commit; refuses if files changed afterwards)
  - `upload_binary`, `batch_read_files`, `repository_tree`, `download_repository`, `compare_branches`, `commit_history_diff`
- **CI and releases.** `workflow_status` (failed jobs plus log tail in one call), runs, jobs, logs, artifacts, releases, `release_from_artifact` (build artifact to a release with a permanent link, done server-side).
- **Railway.** Projects, deployments, logs, variables (names only by default), set variable, redeploy, restart, `railway_service_status`, raw GraphQL.
- **Post-deploy checks.** `run_post_deploy_tests` calls your site or API from the server (status, body text, JSON fields, latency). Private and internal addresses are blocked (SSRF protection).
- **Confirmed deletions.** Deleting a Railway service or project, or a GitHub branch, is a two-step operation: the first call returns a plan and a one-time code, the second call with the code executes it.
- **Tool annotations** (`readOnlyHint`, `destructiveHint`, `openWorldHint`) on every tool.
- A generic `github_api` tool covers any other GitHub REST endpoint.

## How it works

```
Claude  --MCP over HTTPS-->  /c/<secret>/mcp  (this app)  -->  GitHub API / Railway GraphQL
                              |
                              +-- website: Google sign-in, accounts, links, permissions
```

- Account tokens are stored **encrypted** (Fernet) in Postgres. Connector links are stored only as a **SHA-256 hash**, so a link is shown once.
- Core is `app.py`. Extra tool modules (`gitx.py`, `ops.py`, `safe.py`, `profiles.py`) are loaded by `main.py` one by one; if one fails to import, the rest of the connector still works.

## Quick start (Railway)

1. Fork or clone this repo and deploy it on Railway (**Deploy from GitHub repo**). The `Procfile` runs `uvicorn main:application`.
2. Add a **PostgreSQL** database and reference its `DATABASE_URL` in the app service.
3. Generate a public domain and set `BASE_URL` to it (no trailing slash).
4. Create a **Google OAuth client** (redirect URI `https://YOUR_DOMAIN/auth/google/callback`) and a **GitHub OAuth app** (callback `https://YOUR_DOMAIN/auth/github/callback`).
5. Set the environment variables below, open the site, sign in, add accounts, create a link, and paste it into Claude.

| Variable | Meaning |
|---|---|
| `BASE_URL` | `https://YOUR_DOMAIN`, no trailing slash |
| `SECRET_KEY` | long random string (session signing) |
| `ENCRYPTION_KEY` | Fernet key: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` (do not lose it) |
| `DATABASE_URL` | Postgres URL |
| `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET` | Google OAuth client |
| `GITHUB_CLIENT_ID`, `GITHUB_CLIENT_SECRET` | GitHub OAuth app |
| `ALLOWED_EMAILS` | optional, comma-separated emails allowed to sign in. **Set this** unless you want anyone to be able to register |

GitHub accounts can be added through OAuth or with a personal access token (scope `repo`). Railway needs an account or workspace token (Account Settings, Tokens). The `mcp` package is pinned below version 2 on purpose.

## Security model (read this)

- A connector link **is a credential**. Anyone who has it can use the accounts and rights you enabled for it. Do not share it; revoke it if it leaks.
- This app holds your GitHub and Railway tokens. Host it yourself, restrict sign-in with `ALLOWED_EMAILS`, and prefer tokens with minimal scopes.
- Permissions are enforced on the server per link and per account; read-only links cannot write even if a tool is called by name.
- There is **no OAuth for the MCP connection itself** (the secret link is the auth). That is fine for a personal tool, and it is why this is not suitable for a public connector directory as-is.
- `github_api` and `railway_graphql` can do anything the underlying token allows; hide the "advanced" group on links where that is too much.
- Confirmation codes protect against accidental one-shot deletions, not against a malicious client that chains both calls.

## Status and limitations

- Used daily by the author on a few accounts. Core read and write tools, multi-account, per-link permissions, tool sets, batch commits, edits and rollback were exercised against the real GitHub API.
- Release/artifact tools, post-deploy checks and the deletion tools are covered by tests against mocked services; they have had less real-world use, so expect rough edges.
- No automated test suite is included in this repo yet.
- Large binaries (APK/ZIP) are never passed through the chat: the server hands out temporary or permanent links instead.

Issues and pull requests are welcome, but this is a hobby project, so replies may be slow.

## License

MIT, see [LICENSE](LICENSE).

---

## Русский

**DevOps для ИИ: личный MCP-сервер, через который Claude работает с твоими GitHub и Railway.** Читает код, делает безопасные коммиты сразу многих файлов, смотрит сборки и логи, откатывает изменения, проверяет сайт после деплоя.

> **Это личный проект.** Я сделал его для себя и выкладываю как есть: без аудита и без гарантий, к Anthropic, OpenAI, GitHub и Railway он отношения не имеет. Разворачивай у себя, читай код и пользуйся на свой риск.

**Что умеет**
- Любое число аккаунтов GitHub и Railway, у каждого своя метка.
- Права у каждой ссылки свои: по каждому аккаунту нет доступа, только чтение или чтение и запись. А ещё для каждой ссылки выбирается набор инструментов, которые видит Claude (меньше инструментов: меньше токенов и путаницы).
- Массовые коммиты одним запросом, точечные правки с проверкой конфликтов (при конфликте не пишется ничего), предпросмотр изменений, безопасный откат новым коммитом.
- Статус сборок и логи упавших джобов одним вызовом, релизы из артефактов сборки, состояние сервисов Railway, проверка сайта после деплоя.
- Удаление сервисов, проектов и веток только в два шага: сначала план и одноразовый код, потом выполнение.

**Запуск:** разверни на Railway (подключи Postgres, задай переменные из таблицы выше, создай OAuth-приложения Google и GitHub), войди на сайт, добавь аккаунты, создай ссылку и вставь её в Claude (Settings, Connectors, Add custom connector).

**Безопасность:** ссылка коннектора это пароль, никому её не давай. Токены аккаунтов хранятся зашифрованными, ссылки только в виде хеша. Обязательно задай `ALLOWED_EMAILS`, иначе зарегистрироваться сможет любой.

**Статус:** хобби-проект. Основное проверено на реальном GitHub; релизы, проверки после деплоя и удаления проверены только на имитации, возможны шероховатости.
