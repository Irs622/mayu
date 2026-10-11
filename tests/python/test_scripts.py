import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]


def test_matrix_rejects_unknown_leg():
    r = subprocess.run([str(ROOT / "scripts/matrix.sh"), "bogus"], capture_output=True, text=True, timeout=10)
    assert r.returncode == 2
    assert "unknown leg: bogus" in r.stderr
