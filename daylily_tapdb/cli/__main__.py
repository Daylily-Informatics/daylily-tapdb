"""Allow running CLI as module: python -m daylily_tapdb.cli"""
from daylily_tapdb.cli import main

if __name__ == "__main__":
    raise SystemExit(main())

