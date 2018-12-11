# -*- coding: utf-8 -*-
import argparse
import logging

from gdb_sync.script import synchronize


def get_version():
    """Retrieves the version for the package"""
    import pkg_resources

    try:
        packages = pkg_resources.require("gdb-sync")
        return packages[0].version
    except:
        pass

    return "dev"


def schema_sync(args):
    if args.verbose:
        # set log level of root logger to debug
        logging.root.setLevel(logging.DEBUG)

    synchronize(args.source, args.target, db_uri=args.db_uri or None)


def main():
    parser = argparse.ArgumentParser(
        prog="gdb-sync", description="GDB Synchronization Script"
    )
    parser.set_defaults(func=lambda args: parser.print_help())
    parser.add_argument(
        "--version", action="version", version="%(prog)s {}".format(get_version())
    )

    subparsers = parser.add_subparsers(title="Commands")
    syncer = subparsers.add_parser("sync", description="run synchronization")
    syncer.add_argument("source", help="source schema (Kaduna checkout from Kano GDB)")
    syncer.add_argument(
        "target", help="target schema (full checkout from Kaduna database)"
    )
    syncer.add_argument(
        "--db-uri",
        dest="db_uri",
        help="the database connection string eg. postgresql://user:pass@host:port/database",
    )
    syncer.add_argument(
        "--verbose", action="store_const", const=True, help="enable verbose logging"
    )
    syncer.set_defaults(func=schema_sync)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
