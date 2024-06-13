# ruff: noqa: PERF203

import sys
from pathlib import Path

import json5


def main():
    for filename in sys.argv[1:]:
        try:
            with Path(filename).open() as file:
                json5.load(file)
            print(f"{filename}: Valid JSONC")
        except Exception as e:
            print(f"{filename}: Invalid JSONC - {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
