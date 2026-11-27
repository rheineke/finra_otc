from datetime import date
from pathlib import Path
from typing import Any

import pytest

from finra_otc import api, build, cdn, cli
from finra_otc.raw import RawStore


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


def test_build_dataset_builds_only_the_api_part(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from finra_otc.raw import RawStore

    store = RawStore(tmp_path)
    store.save("cdn/regsho_daily", "x", [("x.txt", b"bad", {})],
               meta={"facility": "CNMS", "date": "2026-10-22"})  # fmt: skip
    assert cli.main(["--data", str(tmp_path), "build", "--dataset", "regShoDaily"]) == 0
    assert cli.main(["--data", str(tmp_path), "build"]) == 1  # the daily part runs


def test_build_fails_while_another_build_runs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with build.exclusive(RawStore(tmp_path)):
        assert cli.main(["--data", str(tmp_path), "build"]) == 1
    assert "another build is running" in capsys.readouterr().err
    assert cli.main(["--data", str(tmp_path), "build"]) == 0


@pytest.mark.parametrize(
    ("failing", "code"),
    [
        ({}, 0),
        ({"weeklySummary": "errors"}, 1),  # a partition failed
        ({"weeklyDownloadDetails": "raise"}, 1),  # its catalog failed
    ],
)
def test_capture_api_exit_status_and_options(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing: dict[str, str],
    code: int,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def capture_dataset(
        client: Any, store: RawStore, dataset: api.Dataset, **kwargs: Any
    ) -> api.Result:
        calls.append((dataset.name, kwargs))
        how = failing.get(dataset.name)
        if how == "raise":
            raise api.CaptureError("catalog down")
        r = api.Result(dataset.name, planned=1, saved=1)
        if how == "errors":
            r.failed, r.errors = 1, ["2026-09-14_T1: short page"]
        return r

    monkeypatch.setattr(cli, "RequestsClient", lambda: object())
    monkeypatch.setattr(api, "capture_dataset", capture_dataset)
    argv = ["--data", str(tmp_path), "-q", "capture", "api", "weeklySummary"]
    argv += ["--accept-empty", "--refresh-since", "2026-01-05", "--workers", "2"]
    assert cli.main(argv) == code
    # Its details dataset first, and still the dataset after a failure.
    assert [name for name, _ in calls] == ["weeklyDownloadDetails", "weeklySummary"]
    options = calls[1][1]
    assert options["accept_empty"] is True and options["workers"] == 2
    assert options["refresh_since"] == date(2026, 1, 5)


@pytest.mark.parametrize("errors", [[], ["CNMS 2026-11-20: missing"]])
def test_capture_regsho_daily_exit_status_and_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, errors: list[str]
) -> None:
    seen: dict[str, Any] = {}

    def capture_daily(client: Any, store: RawStore, **kwargs: Any) -> cdn.Result:
        seen.update(kwargs)
        return cdn.Result(cdn.DAILY_SOURCE, failed=len(errors), errors=errors)

    monkeypatch.setattr(cli, "RequestsClient", lambda: object())
    monkeypatch.setattr(cdn, "capture_daily", capture_daily)
    argv = ["--data", str(tmp_path), "-q", "capture", "regsho-daily", "--facility",
            "CNMS", "--refresh-since", "2026-11-02", "--recheck-absent"]  # fmt: skip
    assert cli.main(argv) == (1 if errors else 0)
    assert seen["facilities"] == ["CNMS"] and seen["recheck_absent"] is True
    assert seen["refresh_since"] == date(2026, 11, 2)


@pytest.mark.parametrize("errors", [[], ["FNYXsh202609.zip: cut short"]])
def test_capture_regsho_monthly_exit_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, errors: list[str]
) -> None:
    def capture_monthly(client: Any, store: RawStore, **kwargs: Any) -> cdn.Result:
        assert kwargs["since"] == date(2026, 9, 1) and kwargs["dry_run"] is False
        return cdn.Result(cdn.MONTHLY_SOURCE, failed=len(errors), errors=errors)

    monkeypatch.setattr(cli, "RequestsClient", lambda: object())
    monkeypatch.setattr(cdn, "capture_monthly", capture_monthly)
    argv = ["--data", str(tmp_path), "-q", "capture", "regsho-monthly", "--since",
            "2026-09"]  # fmt: skip
    assert cli.main(argv) == (1 if errors else 0)


def test_capture_legacy_fails_on_names_it_doesnt_know(tmp_path: Path) -> None:
    folder = tmp_path / "old"
    (folder / "2016").mkdir(parents=True)
    (folder / "2016" / "notes.csv").write_text("x")
    argv = ["--data", str(tmp_path / "data"), "-q", "capture", "legacy", str(folder)]
    assert cli.main(argv) == 1


def test_status_recover(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store = RawStore(tmp_path)
    lost = store.save("s", "k", [("f", b"a", {})])
    store.manifest_path("s").write_text("")
    assert cli.main(["--data", str(tmp_path), "status"]) == 0
    assert "run status --recover" in capsys.readouterr().out
    assert cli.main(["--data", str(tmp_path), "status", "--recover"]) == 0
    assert "recorded again: 1" in capsys.readouterr().out
    assert store.latest("s") == {"k": lost}
    # A folder it can't check stays listed, and fails the command.
    store.path(lost.files[0]).parent.joinpath(".capture.json").unlink()
    store.manifest_path("s").write_text("")
    assert cli.main(["--data", str(tmp_path), "status", "--recover"]) == 1
    assert "can't be recovered" in capsys.readouterr().out
