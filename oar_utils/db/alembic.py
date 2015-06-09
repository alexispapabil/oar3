# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals


from sqlalchemy import MetaData

from oar.lib.compat import to_unicode

from .helpers import yellow, blue, red

from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.autogenerate import compare_metadata


SUPPORTED_ALEMBIC_OPERATIONS = [
    'add_table',
    'add_column',
    'remove_column',
    'modify_nullable',
    'modify_type',
    # 'add_index',
    # 'remove_index',
]


def alembic_generate_diff(from_engine, to_engine):
    mc = MigrationContext.configure(to_engine.connect())
    from_metadata = MetaData()
    from_metadata.reflect(bind=from_engine)

    for diff in compare_metadata(mc, from_metadata):
        if isinstance(diff[0], tuple):
            op_name = diff[0][0]
        else:
            op_name = diff[0]
        if op_name in SUPPORTED_ALEMBIC_OPERATIONS:
            yield op_name, diff


def alembic_apply_diff(ctx, op, op_name, diff):
    if op_name not in (SUPPORTED_ALEMBIC_OPERATIONS):
        raise ValueError("Unsupported '%s' operation" % op_name)

    if op_name == "add_table":
        table_name = diff[1].name
        columns = [c for c in diff[1].columns]
        for column in columns:
            column.table = None
        msg = "create table %s" % table_name
        op_callback = lambda: op.create_table(table_name, *columns)
    elif op_name in ('add_column', 'remove_column'):
        column = diff[3]
        column.table = None
        if 'add' in op_name:
            table_name = 'to table %s' % diff[2]
            op_callback = lambda: op.add_column(diff[2], column)
        else:
            table_name = 'from table %s' % diff[2]
            op_callback = lambda: op.drop_column(diff[2], column.name)
        msg = '%s %s %s' % (op_name, column.name, table_name)
    elif op_name in ('remove_index', 'add_index'):
        index = diff[1]
        columns = [i for i in index.columns]
        table_name = index.table.name
        index_colums = ()
        for column in columns:
            index_colums += ("%s.%s" % (column.table.name, column.name),)
        #import ipdb; ipdb.set_trace()  # noqa
        if 'add' in op_name:
            args = (index.name, table_name, [c.name for c in columns],)
            kwargs = {'unique': index.unique}
            op_callback = lambda: op.create_index(*args, **kwargs)
        else:
            op_callback = lambda: op.drop_index(index.name)
        msg = '%s %s for columns (%s)' % (op_name, index.name,
                                          ",".join(index_colums))
    elif op_name in ('modify_nullable',):
        table_name = diff[0][2]
        column_name = diff[0][3]
        kwargs = diff[0][4]
        nullable = diff[0][6]

        def op_callback():
            op.alter_column(table_name, column_name,
                            nullable=nullable, **kwargs)
        msg = "set %s.%s nullable to %s" % (table_name, column_name, nullable)
    try:
        if msg is not None:
            ctx.log("%s ~> %s" % (yellow('upgrade'), msg))
        op_callback()
    except Exception as ex:
        ctx.log(*red(to_unicode(ex)).splitlines(), prefix=(' ' * 9))


def alembic_sync_schema(ctx, from_engine, to_engine):
    # ctx.current_db.reflect()
    message = blue('compare') + ' ~> databases schemes'
    ctx.log(message + ' (in progress)')
    diffs = list(alembic_generate_diff(from_engine, to_engine))

    ctx.log("%s (%s)" % (message, blue("%s changes" % len(diffs))))

    mc = MigrationContext.configure(to_engine.connect())
    op = Operations(mc)

    for op_name, diff in diffs:
        alembic_apply_diff(ctx, op, op_name, diff)
