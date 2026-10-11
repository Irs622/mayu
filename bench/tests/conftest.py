import pathlib
import sys

BENCH = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(BENCH), str(BENCH / "systems")]
