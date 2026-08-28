"""The dashboard's SQLite store.

Shared by two processes with different jobs and different virtualenvs:

* the **agents** write what they poll, as they poll it, so history accumulates;
* the **API** reads, and can answer even when an agent is down.

Stdlib `sqlite3` only. The agents run inside the trading virtualenv — the one
that places real orders — and that environment gains no dependency for this.
"""
from webapp.store.schema import SCHEMA_VERSION, connect, migrate  # noqa: F401
