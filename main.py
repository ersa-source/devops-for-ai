"""Точка входа. Ядро (app.py) всегда запускается; расширения подключаются по одному,
и если какой-то модуль не загрузился, это попадает в лог, а остальной коннектор работает как раньше."""
import importlib
import logging

import app as core

log = logging.getLogger("ghconn")
LOADED, FAILED = [], {}

for _name in ("gitx", "ops", "safe", "profiles"):
    try:
        importlib.import_module(_name)
        LOADED.append(_name)
    except Exception as ex:  # noqa: BLE001
        FAILED[_name] = f"{type(ex).__name__}: {ex}"
        log.exception("extension %s failed to load", _name)

print(f"[ghconn] extensions loaded: {LOADED}; failed: {FAILED}", flush=True)

# FastMCP регистрирует инструменты при импорте модулей выше, а streamable_http_app уже создан из того же
# объекта mcp, поэтому новые инструменты видны сразу.
application = core.application
