# -*- coding: utf-8 -*-
import logging
import math
import os
import time

import psycopg2
import psycopg2.sql as sql
from psycopg2.extras import NamedTupleCursor

log = logging.getLogger(__name__)


class Synchronizer(object):
    """The Synchronizer manages database synchronization between two schemas
    
    Args:
        source_schema (str): source schema to sync from.
        target_schema (str): target schema to sync to.
        db_uri (str, optional): connection string for initializing database connection.
            If not set, the db_uri will be read from the environment variable `DATABASE_URI`
    
    Attributes:
        source (str): the source schema.
        target (str): the target schema.
    """

    def __init__(self, source_schema: str, target_schema: str, db_uri: str = None):
        timestamp = math.floor(time.time())
        self.source = source_schema.lower()
        self.target = target_schema.lower()
        self.__temp_schema = f"gdb_sync_temp_{timestamp}"
        self._dsn = db_uri or os.getenv("DATABASE_URI")

    def _is_connected(self) -> bool:
        if self.connection and isinstance(
            self.connection, psycopg2.extensions.connection
        ):
            return True

        log.warn(f"{type(self).__name__}.connection is not a valid psycopg2 connection")

        return False

    @property
    def connection(self) -> psycopg2.extensions.connection:
        """psycopg2.extensions.connection: database connection object"""
        pgconn = getattr(self, "__pgconn", None)
        if isinstance(pgconn, psycopg2.extensions.connection):
            return pgconn

        try:
            self.__pgconn = psycopg2.connect(self._dsn, cursor_factory=NamedTupleCursor)
            return self.__pgconn
        except Exception as e:
            log.error(e)

        return None

    def get_table_names(self, schema: str) -> set:
        """Get the table/view names for a given schema.

        Args:
            schema (str): the name of the schema to search

        Returns:
            set: the set of distinct table names (or view names) in the schema, returns empty set
                if database connection cannot be made.
        """
        table_names = set()
        if not self._is_connected():
            return table_names

        query = sql.SQL(
            """SELECT table_schema || '.' || table_name AS rel_name FROM information_schema.tables
            WHERE table_schema = {schema} AND table_type = 'BASE TABLE'
            UNION
            SELECT table_schema || '.' || table_name AS rel_name FROM information_schema.views WHERE table_schema = {schema};
            """
        ).format(schema=sql.Literal(schema))

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor.fetchall():
                table_names.add(row.rel_name)

        return table_names

    def get_column_names(self, table_name: str) -> set:
        """Get the column names for a given table.

        Args:
            table_name (str): the name of the table or view to search

        Returns:
            set: the set of distinct column names in the table or view, returns empty set
                if database connection cannot be made.
        """
        column_names = set()
        if not self._is_connected():
            return column_names

        query = sql.SQL("SELECT * FROM {} WHERE False;").format(
            sql.Identifier(table_name)
        )
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            column_names = set(desc.name for desc in cursor.description)

        return column_names

    def is_compatible(self, source_table: str, destination_table: str) -> bool:
        """Check if two tables can be merged. Two tables are compatible if they both
        have a `globalid` (or `global_id`) column and the set of columns in the destination
        table is a subset of the columns in the source table.

        Args:
            source_table (str): the name of the table or view from which data is to be copied
            destination_table (str): the name of the destination table

        Returns:
            bool: returns true if the tables are compatible, false otherwise
        """
        source_columns = self.get_column_names(source_table)
        destination_columns = self.get_column_names(destination_table)

        if len(source_columns) and len(destination_columns):
            if destination_columns.issubset(source_columns):
                has_globalid = (
                    "globalid" in source_columns and "globalid" in destination_columns
                )
                has_global_id = (
                    "global_id" in source_columns and "global_id" in destination_columns
                )
                return has_globalid or has_global_id

        return False

    def exists(self, table_name: str) -> bool:
        """Check if a given table exists. The table name should be in the form
        `schema_name`.`table_name`

        Args:
            table_name (str): the name of the table (prefixed by the table schema).
        
        Returns:
            bool: True if the table exists, False otherwise.
        
        Raises:
            ValueError: raised when table name is invalid
        """
        if "." not in table_name:
            raise ValueError(
                "table_name must be a full relation name in the form `schema`.`table`."
            )

        schema, table = [sql.Literal(i) for i in table_name.split(".")]
        query = sql.SQL(
            """
            SELECT * FROM information_schema.tables WHERE table_schema = {} AND table_name = {};
            """
        ).format(schema, table)

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.rowcount > 0

    def copy_table(self, source_table: str, target_table: str, overwrite: bool = False):
        """Copy a table or view from one schema to another. If the target table exists
        do an UPSERT.

        Args:
            source_table (str): the source table to copy from.
            target_table (str): the destination table.
            overwrite (bool): indicates whether to override existing table or not.
        """
        source = sql.Identifier(source_table)
        target = sql.Identifier(target_table)
        query = sql.SQL("CREATE TABLE {} AS SELECT * FROM {};").format(target, source)

        if self.exists(target_table):
            if self.is_compatible(source_table, target_table):
                column_names = self.get_column_names(target_table)
                guid = sql.Identifier(
                    "globalid" if "globalid" in column_names else "global_id"
                )
                columns = sql.SQL(", ").join(sql.Identifier(c) for c in column_names)
                query = sql.SQL(
                    """INSERT INTO {target} ({columns}) SELECT {columns} FROM {source}
                    ON CONFLICT ({guid}) DO UPDATE SET {guid} = EXCLUDED.{guid};"""
                ).format(source=source, target=target, columns=columns, guid=guid)
            else:
                if not overwrite:
                    log.warning(
                        (
                            "The target table exists but is not compatible with the "
                            "source table. If you wish to overwrite the table set "
                            f"`overwrite` to True. Source: {source_table} Target: {target_table}."
                        )
                    )
                    return
                query = sql.SQL(
                    "DROP TABLE {target} CASCADE; CREATE TABLE {target} AS SELECT * FROM {source};"
                ).format(source=source, target=target)

        with self.connection.cursor() as cursor:
            cursor.execute(query)

        self.connection.commit()

    def synchronize(self):
        """Run the schema synchronization operation on the schemas."""
        try:
            log.info(f"Copying data from schema {self.source!r} to {self.target!r}.")

            for source_table in self.get_table_names(self.source):
                table_name = source_table.split(".")[-1]
                target_table = ".".join([self.target, table_name])

                try:
                    log.info(f"Copying {source_table!r} into {target_table!r}.")
                    self.copy_table(source_table, target_table, overwrite=True)
                except Exception as e:
                    log.error(
                        f"Failed to copy {source_table!r} into {target_table!r}",
                        exc_info=True,
                    )

            log.info(f"Schema {self.source!r} has been merged into {self.target!r}.")

        except Exception as e:
            log.error(e)


def synchronize(source_schema, target_schema, db_uri=None):
    """Entrypoint for initializing and running the Synchronizer"""
    if not all(
        isinstance(i, str) for i in [source_schema, target_schema, db_uri or ""]
    ):
        raise ValueError("String expected.")

    Synchronizer(source_schema, target_schema, db_uri).synchronize()
