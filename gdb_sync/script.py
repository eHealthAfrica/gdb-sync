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

    def get_table_names(self, schema: str, rel_type: str = "table") -> set:
        """Get the table names for a given schema.

        Args:
            schema (str): the name of the schema to search
            rel_type (str, optional): the relation type, must be one of `view` or `table`

        Returns:
            set: the set of distinct table names (or view names) in the schema, returns empty set
                if database connection cannot be made.
        """
        table_names = set()
        if not self._is_connected():
            return table_names

        query = sql.SQL(
            """SELECT table_schema || '.' || table_name AS rel_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE';"""
        ).format(sql.Literal(schema))

        if rel_type is not "table":
            query = sql.SQL(
                """SELECT table_schema || '.' || table_name AS rel_name
                FROM information_schema.views WHERE table_schema = %s"""
            ).format(sql.Literal(schema))

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

        query = sql.SQL("SELECT * FROM {} WHERE false;").format(
            sql.Identifier(table_name)
        )
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            column_names = set(desc[0] for desc in cursor.description)

        return column_names

    def create_temp_schema(self) -> bool:
        """Create a temporary schema where the syncing will be done.
        
        Returns:
            bool: return True if the temporary schema was successfully created, false otherwise.
        """
        if not self._is_connected():
            return False

        schema = sql.Identifier(self.__temp_schema)
        query = sql.SQL(
            "DROP SCHEMA IF EXISTS {} CASCADE; CREATE SCHEMA IF NOT EXISTS {};"
        ).format(schema, schema)

        with self.connection.cursor() as cursor:
            cursor.execute(query)

        self.connection.commit()
        return True

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

    def copy_table(
        self, source_table: str, destination_table: str, overwrite: bool = False
    ):
        """Copy a table or view from one schema to another.

        Args:
            source_table (str): the source table to copy from.
            destination_table (str): the destination table.
            overwrite (bool): a boolean indicating whether or not overwriting existing
                table is allowed.
        """
        source = sql.Identifier(source_table)
        destination = sql.Identifier(destination_table)
        query = sql.SQL("CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM {};").format(
            destination, source
        )

        if overwrite:
            query = sql.SQL(
                "DROP TABLE IF EXISTS {} CASCADE; CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM {};"
            ).format(destination, destination, source)

        with self.connection.cursor() as cursor:
            cursor.execute(query)

        self.connection.commit()

    def copy_data(self, source_table: str, destination_table: str):
        """Copy data from a table or view in one schema to a table in another schema.
        
        Args:
            source_table (str): the source table to copy from.
            destination_table (str): the destination table.
        """
        if not self.is_compatible(source_table, destination_table):
            log.info(
                f"{source_table} and {destination_table} are not compatible, skipping."
            )
            return

        column_names = self.get_column_names(destination_table)
        globalid = sql.Identifier(
            "globalid" if "globalid" in column_names else "global_id"
        )
        columns = sql.SQL(", ").join(sql.Identifier(c) for c in column_names)
        source = sql.Identifier(source_table)
        destination = sql.Identifier(destination_table)
        query = sql.SQL(
            "INSERT INTO {} ({}) SELECT {} FROM {} WHERE {} NOT IN (SELECT DISTINCT {} FROM {});"
        ).format(destination, columns, columns, source, globalid, globalid, destination)

        with self.connection.cursor() as cursor:
            cursor.execute(query)

        self.connection.commit()

    def synchronize(self):
        """Run the schema synchronization operation on the schemas."""
        try:
            # create a temporary schema
            log.info(
                f"Creating a temporary schema with the name: {self.__temp_schema}."
            )
            created = self.create_temp_schema()
            if not created:
                raise Exception("Cannot create temp schema, quitting...")

            log.info(
                f"Copying existing data in the target schema `{self.target}` to the temporary schema."
            )
            for table in self.get_table_names(self.target):
                table_name = table.split(".")[-1]
                destination_table = ".".join([self.__temp_schema, table_name])

                # copy the data in the destination views to the temporary schema
                log.info(
                    f"Copying view {table} into table `{destination_table}` in the temporary schema."
                )
                self.copy_table(table, destination_table, overwrite=True)

                # copy data which currently exist in the source schema but not in the
                # destination schema into the temporary schema
                source_table = ".".join([self.source, table_name])
                log.info(
                    f"Copying new data from source view {source_table} into table `{destination_table}` in the temporary schema."
                )
                self.copy_data(source_table, destination_table)

                # @todo: should we delete source and target schema? should we rename temporary schema?

            log.info(
                (
                    f"The data in the source schema `{self.source}` and target schema `{self.target}`",
                    f" have been successfully merged into a new schema with name `{self.__temp_schema}`.",
                )
            )

        except Exception as e:
            log.error(e)


def synchronize(source_schema, target_schema, db_uri=None):
    """Entrypoint for initializing and running the Synchronizer"""
    if not all(
        isinstance(i, str) for i in [source_schema, target_schema, db_uri or ""]
    ):
        raise ValueError("String expected.")

    Synchronizer(source_schema, target_schema, db_uri).synchronize()
