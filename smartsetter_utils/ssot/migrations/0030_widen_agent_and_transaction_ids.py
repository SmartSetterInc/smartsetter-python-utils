"""
Widen Agent.id and Transaction.id from varchar(32) to varchar(64), together with
every column that stores one of them through a foreign key.

Restats ids are "<key>__<board>" (realitydb.restats_api.namespaced_key), and long
board names such as GBRMLSGBRARNOMARROAM push agent ids past 32 characters.

Why this is not a plain AlterField:
- Django's AlterField drops and re-adds every foreign key pointing at the primary
  key, and re-adding validates it by scanning the referencing tables (the
  transactions table has tens of millions of rows) under lock.
- A varchar length increase is binary-coercible in Postgres, so ALTER COLUMN TYPE
  neither rewrites the table nor rebuilds its indexes, and the foreign keys are
  re-attached without being revalidated. The migration therefore issues the
  ALTERs itself (database side) and records the AlterFields (state side).
- Every MLS has a materialized view "SELECT * FROM ssot_agent WHERE mls_id=...".
  Postgres refuses to change the type of a column that a view uses, so those views
  are dropped (before the base tables are locked) and recreated (WITH NO DATA,
  with their indexes, comment and grants) inside the same transaction. Filling
  them is the slow part (about 4 minutes for 114 views on dev), so it is left
  to the next migration, which refreshes them one at a time after this
  transaction has released its locks. Until then an MLS agent list
  ("My MLS") whose view is not refilled yet raises "materialized view has not
  been populated".

The tables are locked in one transaction with a short lock_timeout. If a sync job
or a long query holds one of them, the attempt is rolled back to a savepoint
(which releases its locks and changes nothing) and retried a few times; after the
last attempt the migration fails without having changed anything.

Reverse: narrowing back to 32 would rewrite the tables and fail on any id longer
than 32 characters that was saved since, so the reverse leaves the database
columns at 64 (which the old code reads and writes without any problem) and only
rolls back the migration state.
"""

import logging
import random
import re
import time

from django.db import OperationalError, migrations, models, transaction

logger = logging.getLogger(__name__)

NEW_LENGTH = 64
LOCK_TIMEOUT = "2s"
LOCK_ATTEMPTS = 20
RETRY_PAUSE_SECONDS = 5
# lock_not_available (lock_timeout hit) and deadlock_detected.
RETRYABLE_PGCODES = ("55P03", "40P01")

# Tables whose "id" is widened. Every column with a foreign key to one of them
# is found at run time, so tables owned by other apps (the backend's realitydb
# and clients tables) are covered too.
PRIMARY_KEY_TABLES = ("ssot_agent", "ssot_transaction")


def _quote(cursor, name):
    cursor.execute("SELECT quote_ident(%s)", [name])
    return cursor.fetchone()[0]


def _id_columns(cursor):
    """
    [(table_oid, qualified_table, column, attnum, type)] for the primary keys and
    every column with a foreign key to them, wide or not. Partitions are left
    out: altering the parent table changes them.
    """
    cursor.execute(
        """
        WITH targets AS (
            SELECT to_regclass(t) AS oid FROM unnest(%s::text[]) AS t
        ), cols AS (
            SELECT t.oid AS relid, a.attnum
            FROM targets t
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attname = 'id'
            UNION
            SELECT k.conrelid, unnest(k.conkey)
            FROM pg_constraint k
            WHERE k.contype = 'f' AND k.confrelid IN (SELECT oid FROM targets)
        )
        SELECT c.oid,
               quote_ident(n.nspname) || '.' || quote_ident(c.relname),
               a.attname, a.attnum, format_type(a.atttypid, a.atttypmod)
        FROM cols
        JOIN pg_class c ON c.oid = cols.relid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = cols.relid AND a.attnum = cols.attnum
        WHERE NOT a.attisdropped AND NOT c.relispartition
        ORDER BY c.oid, a.attnum
        """,
        [list(PRIMARY_KEY_TABLES)],
    )
    return cursor.fetchall()


def _needs_widening(type_name):
    # Only bounded varchar can be too narrow; text or unbounded varchar is fine.
    match = re.fullmatch(r"character varying\((\d+)\)", type_name)
    return bool(match) and int(match.group(1)) < NEW_LENGTH


def _dependent_views(cursor, columns):
    """oid -> relkind of every view or materialized view using one of the columns."""
    views = {}
    for table_oid, _, _, attnum, _ in columns:
        cursor.execute(
            """
            SELECT DISTINCT v.oid, v.relkind
            FROM pg_depend d
            JOIN pg_rewrite r ON r.oid = d.objid
            JOIN pg_class v ON v.oid = r.ev_class
            WHERE d.classid = 'pg_rewrite'::regclass
              AND d.refclassid = 'pg_class'::regclass
              AND d.refobjid = %s
              AND d.refobjsubid = %s
              AND v.oid <> %s
            """,
            [table_oid, attnum, table_oid],
        )
        views.update(cursor.fetchall())
    return views


def _capture_view(cursor, oid, kind):
    cursor.execute(
        """
        SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname), n.nspname,
               c.relname, pg_get_userbyid(c.relowner), current_user,
               pg_get_viewdef(c.oid), obj_description(c.oid, 'pg_class')
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = %s
        """,
        [oid],
    )
    qualified, schema, name, owner, current, definition, comment = cursor.fetchone()
    if kind not in ("m", "v"):
        raise RuntimeError(f"Unexpected dependent relation {qualified} (relkind {kind})")
    # Fail closed: a view recreated by another role would change its owner.
    if owner != current:
        raise RuntimeError(
            f"{qualified} is owned by {owner}, not {current}; recreate it by hand"
        )
    cursor.execute(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
        [schema, name],
    )
    indexes = [row[0] for row in cursor.fetchall()]
    cursor.execute(
        """
        SELECT CASE WHEN g.grantee = 0 THEN 'PUBLIC'
                    ELSE quote_ident(pg_get_userbyid(g.grantee)) END,
               g.privilege_type, g.is_grantable
        FROM pg_class c, aclexplode(c.relacl) g
        WHERE c.oid = %s AND g.grantee <> c.relowner
        """,
        [oid],
    )
    grants = cursor.fetchall()
    return {
        "qualified": qualified,
        "keyword": "MATERIALIZED VIEW" if kind == "m" else "VIEW",
        "kind": kind,
        "definition": definition.rstrip().rstrip(";"),
        "indexes": indexes,
        "comment": comment,
        "grants": grants,
    }


def _capture_and_drop(cursor, views):
    captured = [_capture_view(cursor, oid, kind) for oid, kind in views.items()]
    for view in captured:
        # No CASCADE: a view built on top of these must stop the migration.
        cursor.execute(f"DROP {view['keyword']} {view['qualified']}")
    return captured


def _recreate(cursor, view):
    if view["kind"] == "m":
        # Filled by migration 0031, outside this transaction's locks.
        cursor.execute(
            f"CREATE MATERIALIZED VIEW {view['qualified']} AS "
            f"{view['definition']} WITH NO DATA"
        )
    else:
        cursor.execute(f"CREATE VIEW {view['qualified']} AS {view['definition']}")
    for index in view["indexes"]:
        cursor.execute(index)
    if view["comment"] is not None:
        cursor.execute(
            f"COMMENT ON {view['keyword']} {view['qualified']} IS %s", [view["comment"]]
        )
    for grantee, privilege, grantable in view["grants"]:
        option = " WITH GRANT OPTION" if grantable else ""
        cursor.execute(f"GRANT {privilege} ON {view['qualified']} TO {grantee}{option}")


def _widen_once(connection):
    with connection.cursor() as cursor:
        columns = _id_columns(cursor)
        to_widen = [col for col in columns if _needs_widening(col[4])]
        if not to_widen:
            return

        # Drop the dependent views first, before the base tables are locked, so
        # waiting for a long reader of one view never queues the whole site
        # behind this migration's table locks.
        captured = _capture_and_drop(cursor, _dependent_views(cursor, to_widen))

        # Then every table the ALTERs touch, in one statement: the tables being
        # widened and every table with a foreign key to them (re-attaching a
        # foreign key locks the referencing table too, wide or not).
        tables = []
        for _, qualified, _, _, _ in columns:
            if qualified not in tables:
                tables.append(qualified)
        cursor.execute("LOCK TABLE " + ", ".join(tables) + " IN ACCESS EXCLUSIVE MODE")

        # A view created between the first look and the lock (a new MLS).
        captured += _capture_and_drop(cursor, _dependent_views(cursor, to_widen))

        by_table = {}
        for _, qualified, column, _, _ in to_widen:
            by_table.setdefault(qualified, []).append(column)
        for qualified, table_columns in by_table.items():
            alters = ", ".join(
                f"ALTER COLUMN {_quote(cursor, column)} TYPE varchar({NEW_LENGTH})"
                for column in table_columns
            )
            cursor.execute(f"ALTER TABLE {qualified} {alters}")

        for view in captured:
            _recreate(cursor, view)


def widen_ids(apps, schema_editor):
    connection = schema_editor.connection
    if connection.vendor != "postgresql":
        return
    with connection.cursor() as cursor:
        # Local to this migration's transaction.
        cursor.execute("SELECT set_config('lock_timeout', %s, true)", [LOCK_TIMEOUT])
    for attempt in range(1, LOCK_ATTEMPTS + 1):
        try:
            # Savepoint: a failed attempt releases every lock it took.
            with transaction.atomic(using=connection.alias, savepoint=True):
                _widen_once(connection)
            return
        except OperationalError as exc:
            pgcode = getattr(exc.__cause__, "pgcode", None)
            if pgcode not in RETRYABLE_PGCODES or attempt == LOCK_ATTEMPTS:
                raise
            logger.warning(
                "Widening ids: tables busy (%s), attempt %s of %s; retrying",
                pgcode,
                attempt,
                LOCK_ATTEMPTS,
            )
            # Jitter, so a retry doesn't line up with the same periodic job.
            time.sleep(RETRY_PAUSE_SECONDS + random.uniform(0, RETRY_PAUSE_SECONDS))


class Migration(migrations.Migration):

    dependencies = [
        ("ssot", "0029_restats_source_choice"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.AlterField(
                    model_name="agent",
                    name="id",
                    field=models.CharField(
                        max_length=NEW_LENGTH, primary_key=True, serialize=False
                    ),
                ),
                migrations.AlterField(
                    model_name="transaction",
                    name="id",
                    field=models.CharField(
                        max_length=NEW_LENGTH, primary_key=True, serialize=False
                    ),
                ),
            ],
            database_operations=[
                # Reverse is a no-op on purpose: see the module docstring.
                migrations.RunPython(widen_ids, migrations.RunPython.noop),
            ],
        ),
    ]
