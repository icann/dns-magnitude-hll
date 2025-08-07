import argparse
import logging
from pathlib import Path

import cbor2
from pycddl import Schema

DATASET_CDDL = Path(__file__).parent.parent / "schema" / "dataset.cddl"


def dump_cbor(data, indent=0):
    for k, v in data.items():
        if isinstance(v, dict):
            print(" " * indent + f"Key: {k}, Value: (dict)")
            dump_cbor(v, indent + 4)
            continue
        if isinstance(v, bytes):
            v = f"({len(v)} bytes)"
        print(" " * indent + f"Key: {k}, Value: {v}")


def main():
    """Main function"""

    parser = argparse.ArgumentParser(description="Dataset validation tool")

    parser.add_argument("--debug", action="store_true", help="Enable debugging")
    parser.add_argument("input", metavar="filename", nargs="+", help="Input files")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    with open(DATASET_CDDL) as fp:
        dataset_schema = Schema(fp.read())

    for filename in args.input:
        with open(filename, "rb") as fp:
            cbor_data = fp.read()
            data = cbor2.loads(cbor_data)
        if args.debug:
            dump_cbor(data)
        dataset_schema.validate_cbor(cbor_data)


if __name__ == "__main__":
    main()
