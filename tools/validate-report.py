import argparse
import json
import logging
from pathlib import Path

from jsonschema import Draft202012Validator as validator

REPORT_SCHEMA = Path(__file__).parent.parent / "schema" / "report-schema.json"


def main():
    """Main function"""

    parser = argparse.ArgumentParser(description="Report validation tool")

    parser.add_argument("--debug", action="store_true", help="Enable debugging")
    parser.add_argument("input", metavar="filename", nargs="+", help="Input files")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    with open(REPORT_SCHEMA) as fp:
        report_schema = validator(
            schema=json.load(fp),
            format_checker=validator.FORMAT_CHECKER,
        )

    for filename in args.input:
        with open(filename) as fp:
            report_data = json.load(fp)
        if args.debug:
            print(json.dumps(report_data, indent=4))
        report_schema.validate(report_data)


if __name__ == "__main__":
    main()
