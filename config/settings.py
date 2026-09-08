"""Django settings.

Three deliberate departures from a default project, all driven by the same
fact: ``POST /events`` is the hot path and runs on every event in the fleet.

1. ``MIDDLEWARE`` is empty. The default stack (security, sessions, auth, CSRF,
   messages, clickjacking) executes on every request. On an endpoint with no
   session, no user and no HTML, that is pure per-event cost. Authentication
   belongs at the ingress for a service like this, not in the request path.

2. ``INSTALLED_APPS`` is minimal: DRF and this app. No admin, no auth, no
   contenttypes. The ORM is used for exactly one thing, the durable alarm
   record, so only that is paid for.

3. DRF renders JSON only. The browsable API is a development nicety that costs
   content negotiation and template rendering on every response.
"""

from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------- core

SECRET_KEY = os.environ.get("SECRET_KEY", "insecure-development-key-do-not-deploy")
DEBUG = os.environ.get("DEBUG", "0") == "1"
ALLOWED_HOSTS = os.environ.get("ALLOWED_HOSTS", "*").split(",")

INSTALLED_APPS = [
    "rest_framework",
    "api",
]

MIDDLEWARE: list[str] = []

ROOT_URLCONF = "config.urls"
ASGI_APPLICATION = "config.asgi.application"
TEMPLATES: list[dict] = []

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

USE_TZ = True
TIME_ZONE = "UTC"

# ----------------------------------------------------------------- database

# The alarm mirror. SQLite keeps the service clone-and-run; point this at
# Postgres for a real deployment by overriding DATABASE_* below.
#
# WAL journal mode matters here: SQLite's default rollback journal takes a
# database-wide write lock, which would block alarm reads during a burst.
# synchronous=NORMAL is safe under WAL and avoids an fsync per transaction,
# which we do not need because the engine's own write-ahead log is the system
# of record for durability.
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": str(DATA_DIR / "alarms.sqlite3"),
        "OPTIONS": {
            "timeout": 20,
            "init_command": "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;",
        },
    }
}

# -------------------------------------------------------------------- DRF

REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": ["rest_framework.renderers.JSONRenderer"],
    "DEFAULT_PARSER_CLASSES": ["rest_framework.parsers.JSONParser"],
    "DEFAULT_AUTHENTICATION_CLASSES": [],
    "DEFAULT_PERMISSION_CLASSES": ["rest_framework.permissions.AllowAny"],
    # django.contrib.auth is not installed, so DRF must not reach for
    # AnonymousUser when resolving request.user.
    "UNAUTHENTICATED_USER": None,
}

# ------------------------------------------------------------------ logging

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "console": {"format": "%(asctime)s %(levelname)-7s %(name)s | %(message)s"},
    },
    "handlers": {
        "stderr": {"class": "logging.StreamHandler", "formatter": "console"},
    },
    "root": {"handlers": ["stderr"], "level": os.environ.get("LOG_LEVEL", "INFO")},
}

# ---------------------------------------------------------- service toggles

#: Mirror alarms into the relational store. Disabling makes the service pure
#: in-memory plus write-ahead log, which is useful when benchmarking ingest.
ALARM_DB_MIRROR = os.environ.get("ALARM_DB_MIRROR", "1") == "1"
