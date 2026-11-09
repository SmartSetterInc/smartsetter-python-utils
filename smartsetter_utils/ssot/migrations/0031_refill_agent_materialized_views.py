"""
Refill the materialized views that 0030 recreated WITH NO DATA (the MLS agent
views).

Not atomic: each REFRESH commits on its own, so every view is readable again as
soon as it is filled, and only that one view is locked while it fills. Re-running
the migration (or the next MLS post-processing run) fills any that are left.
"""

import logging

from django.db import migrations

logger = logging.getLogger(__name__)


def refill_views(apps, schema_editor):
    connection = schema_editor.connection
    if connection.vendor != "postgresql":
        return
    with connection.cursor() as cursor:
        # Every empty materialized view built on the tables 0030 may have
        # touched: ssot_agent, ssot_transaction and the tables referencing them.
        cursor.execute(
            """
            WITH targets AS (
                SELECT to_regclass(t) AS oid
                FROM unnest(ARRAY['ssot_agent', 'ssot_transaction']) AS t
            ), tables AS (
                SELECT oid FROM targets
                UNION
                SELECT conrelid FROM pg_constraint
                WHERE contype = 'f' AND confrelid IN (SELECT oid FROM targets)
            )
            SELECT DISTINCT n.nspname, v.relname
            FROM pg_depend d
            JOIN pg_rewrite r ON r.oid = d.objid
            JOIN pg_class v ON v.oid = r.ev_class
            JOIN pg_namespace n ON n.oid = v.relnamespace
            WHERE d.classid = 'pg_rewrite'::regclass
              AND d.refobjid IN (SELECT oid FROM tables)
              AND v.relkind = 'm'
              AND NOT v.relispopulated
            ORDER BY 1, 2
            """
        )
        views = cursor.fetchall()
    for schema, name in views:
        with connection.cursor() as cursor:
            # Re-checked: a view whose MLS was deleted meanwhile is gone.
            cursor.execute(
                "SELECT quote_ident(%s) || '.' || quote_ident(%s), "
                "to_regclass(quote_ident(%s) || '.' || quote_ident(%s)) IS NOT NULL",
                [schema, name, schema, name],
            )
            qualified, exists = cursor.fetchone()
            if not exists:
                logger.info("Skipped %s: dropped meanwhile", qualified)
                continue
            cursor.execute(f"REFRESH MATERIALIZED VIEW {qualified}")
            cursor.execute(f"ANALYZE {qualified}")
        logger.info("Refilled %s", qualified)


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("ssot", "0030_widen_agent_and_transaction_ids"),
    ]

    operations = [
        # Nothing to undo: the views are only refilled.
        migrations.RunPython(refill_views, migrations.RunPython.noop),
    ]
