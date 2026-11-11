"""The five product properties of ADR 0040, registered once: each matrix is found on the tree by its `property_<name>` files.

    python3 -m dev.release_oracle.properties --self-test
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

try:
    from .core import ROOT, Ledger, Status
except ImportError:  # pragma: no cover - bare sys.path entry
    from core import ROOT, Ledger, Status  # type: ignore

__all__ = ["PROPERTIES", "HOMES", "matrix_files", "unregistered", "verify_property_matrices", "self_test"]

PROPERTIES = (
    "no_secret_in_output",
    "no_silent_null",
    "check_equals_run",
    "refusal_names_true_cause",
    "no_debris_after_stop",
)
HOMES = ("tests/offline", "tests/live", "dev/release_oracle", "dev/pytools")


def _named(root: Path) -> list[Path]:
    """Every file named `property_*` in a home of a property matrix."""
    return sorted(p for home in HOMES for p in (root / home).glob("property_*") if p.is_file())


def _owner(path: Path) -> str | None:
    """The registered property a `property_*` file belongs to, `None` when it names none."""
    stem = path.stem.removeprefix("property_")
    return next((n for n in PROPERTIES if stem == n or stem.startswith(f"{n}_")), None)


def matrix_files(name: str, root: Path = ROOT) -> list[str]:
    """The files of property `name`'s matrix on the tree, relative to `root`."""
    return [str(p.relative_to(root)) for p in _named(root) if _owner(p) == name]


def unregistered(root: Path = ROOT) -> list[str]:
    """`property_*` files that belong to no registered property."""
    return [str(p.relative_to(root)) for p in _named(root) if _owner(p) is None]


def verify_property_matrices(led: Ledger, root: Path = ROOT) -> None:
    """One row per registered property: FAIL when no matrix of that name is on the tree; FAIL for a `property_*` file no property owns."""
    led.phase("Property matrices on the tree (ADR 0040)")
    for name in PROPERTIES:
        files = matrix_files(name, root)
        if files:
            led.passed("property", name, "matrix on tree", "-", f"property {name}: matrix on the tree ({', '.join(files)})")
        else:
            led.failed("property", name, "matrix on tree", "-",
                       f"property {name}: no property_{name} file under {', '.join(HOMES)}; the 0.33 criterion needs its matrix")
    for path in unregistered(root):
        led.failed("property", "-", "matrix on tree", "-",
                   f"{path} grades no registered property: add its name to PROPERTIES in dev/release_oracle/properties.py or rename it")


def self_test() -> None:
    """A matrix is found by its name, a stray `property_*` file is caught, a missing matrix is a FAIL row; the real tree has no stray file."""
    with tempfile.TemporaryDirectory() as t:
        root = Path(t)
        for rel in ("tests/offline/property_no_silent_null.rs", "tests/live/property_no_silent_null_axes.rs",
                    "dev/pytools/property_check_equals_run.py", "tests/offline/property_no_silent_nullable.rs",
                    "tests/offline/live_cdc_property.rs"):
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text("")
        assert matrix_files("no_silent_null", root) == ["tests/live/property_no_silent_null_axes.rs",
                                                         "tests/offline/property_no_silent_null.rs"], matrix_files("no_silent_null", root)
        assert unregistered(root) == ["tests/offline/property_no_silent_nullable.rs"], unregistered(root)
        led = Ledger(colour=False)
        led._buf = []
        verify_property_matrices(led, root)
        failed = sorted(c.version for c in led.cells if c.status is Status.FAIL)
        assert failed == ["-", "no_debris_after_stop", "no_secret_in_output", "refusal_names_true_cause"], failed
    stray = unregistered()
    assert not stray, f"property_* files that grade no registered property: {stray}"
    print(f"properties self-test: ok ({sum(bool(matrix_files(n)) for n in PROPERTIES)}/{len(PROPERTIES)} matrices on this tree)")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        raise SystemExit(0)
    raise SystemExit(__doc__)
