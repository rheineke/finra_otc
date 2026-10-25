from pathlib import Path

import pytest

from finra_otc import cli


def test_build_and_status_on_an_empty_store(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(["--data", str(tmp_path), "build"]) == 0
    assert cli.main(["--data", str(tmp_path), "status"]) == 0
    assert "source" in capsys.readouterr().out


def test_unknown_dataset_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        cli.main(["--data", str(tmp_path), "capture", "api", "nope"])


def test_unknown_build_part_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        cli.main(["--data", str(tmp_path), "build", "nope"])


def test_monthly_needs_since() -> None:
    with pytest.raises(SystemExit):
        cli.get_parser().parse_args(["capture", "regsho-monthly"])
