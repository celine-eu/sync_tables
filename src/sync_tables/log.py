"""Output that is safe to ship to a shared log (plan S8/W11).

Log lines carry table names, row counts, watermarks, durations and refusal codes. They
never carry a DSN, a host, a credential, a certificate subject, a client address or a
row's values. Database errors are the usual leak: SQLAlchemy appends the statement and
its parameters to the message, and Postgres puts offending values in DETAIL. So errors
are reduced to their class, SQLSTATE and the object names Postgres reports.
"""
from __future__ import annotations

import sys

# SQLSTATEs whose primary message names objects only, never values.
_SAFE_PRIMARY = {
    "2BP01",  # dependent_objects_still_exist
    "25006",  # read_only_sql_transaction
    "42501",  # insufficient_privilege
    "42P01",  # undefined_table
    "42703",  # undefined_column
    "42P07",  # duplicate_table
    "42710",  # duplicate_object
    "3F000",  # invalid_schema_name
    "42883",  # undefined_function
    "42809",  # wrong_object_type
    "0A000",  # feature_not_supported
}
# SQLSTATEs whose DETAIL lists dependent objects by name.
_SAFE_DETAIL = {"2BP01"}


def log(msg: str) -> None:
    print(msg, file=sys.stdout, flush=True)


def safe_error(e: BaseException) -> str:
    """Describe an exception without values, statements, DSNs or hosts."""
    from sqlalchemy.exc import DBAPIError  # noqa: PLC0415

    orig = e.orig if isinstance(e, DBAPIError) else e
    sqlstate = getattr(orig, "sqlstate", None)
    diag = getattr(orig, "diag", None)

    if sqlstate is not None and diag is not None:
        out = f"{type(orig).__name__} [{sqlstate}]"
        if sqlstate in _SAFE_PRIMARY and diag.message_primary:
            out += ": " + diag.message_primary.strip()
        if sqlstate in _SAFE_DETAIL and diag.message_detail:
            out += " — " + diag.message_detail.strip().replace("\n", "; ")
        names = [
            f"{k}={v}"
            for k, v in (
                ("schema", diag.schema_name),
                ("table", diag.table_name),
                ("column", diag.column_name),
                ("constraint", diag.constraint_name),
                ("type", diag.datatype_name),
            )
            if v
        ]
        if names:
            out += " (" + ", ".join(names) + ")"
        return out

    module = type(orig).__module__
    if module == "sqlite3":
        # Local test databases only. SQLite messages name objects, not values.
        return f"{type(orig).__name__}: {str(orig).strip()[:300]}"

    if isinstance(e, SyncError):
        return str(e)

    if isinstance(orig, (FileNotFoundError, PermissionError, IsADirectoryError)):
        return f"{type(orig).__name__}: {orig.filename}"  # a local path, never a secret
    if isinstance(orig, OSError) and orig.strerror:
        return f"{type(orig).__name__}: {orig.strerror}"  # errno text, e.g. "Address already in use"

    # Anything else: connection errors carry the host, driver errors may carry values.
    # The class and where it was raised are enough to find it.
    return type(orig).__name__ + (f" [{sqlstate}]" if sqlstate else "") + _where(e)


def _where(e: BaseException) -> str:
    tb = e.__traceback__
    if tb is None:
        return ""
    while tb.tb_next is not None:
        tb = tb.tb_next
    import os  # noqa: PLC0415

    return f" at {os.path.basename(tb.tb_frame.f_code.co_filename)}:{tb.tb_lineno}"


class SyncError(Exception):
    """An error whose message is written by this tool, and is safe to log."""


class Refused(SyncError):
    """A guard refused the run. `code` is stable and goes on the wire and in logs."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"REFUSED [{code}]: {message}")
        self.code = code
        self.message = message
