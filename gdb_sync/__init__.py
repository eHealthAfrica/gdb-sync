# -*- coding: utf-8 -*-
"""GDB Synchronization Script

This script automates the process of updating the Kaduna database with new data
from Kano GDB.

The script takes as input the names of the source and target schemas. The source
schema is the schema containing a checkout of the Kaduna database from Kano GDB
while the target schema is a schema containing a full checkout of the Kaduna GDB.

The views in the target schema are copied into tables in a staging schema. Then,
all records on the views in the source schema which do not exist in the target
schema are inserted into the tables in the staging schema. The staging schema is
then exported to be used for updating the Kaduna database.

Usage:
    The script can be called from the command line using the following command:
    
        $ gdb-sync source_schema target_schema --db-uri="postgresql://user:pass@host:port/database"
    
    The optional `--db-uri` parameter may be set via the environment variable `DATABASE_URI`.
"""
import logging

__version__ = "0.1.0"

logging.basicConfig(
    filename="gdb-sync.log",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
