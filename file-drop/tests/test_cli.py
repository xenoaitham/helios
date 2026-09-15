"""CLI tests: main() end-to-end against tmp dirs, flag validation, safe paths."""

from __future__ import annotations

import pytest

from filedrop.generate import main


def test_cli_clean_run_writes_and_reports(tmp_path, capsys):
    rc = main(["--drop-dir", str(tmp_path), "--for-date", "20260909", "--clean", "--report"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "dirt=none" in out
    assert (tmp_path / "customers-20260909.csv").stat().st_size > 0
    assert (tmp_path / "products-20260909.csv").stat().st_size > 0


def test_cli_default_dirt_includes_all_modes(tmp_path, capsys):
    rc = main(["--drop-dir", str(tmp_path), "--for-date", "20260910", "--report"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "dupes_added=" in out and "ragged_lines=" in out and "encoding_lines=1" in out


def test_cli_late_offset_flags_the_file_as_late(tmp_path, capsys):
    rc = main(["--drop-dir", str(tmp_path), "--for-date", "20260909", "--late-offset", "2", "--report"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "(LATE)" in out
    assert (tmp_path / "customers-20260907.csv").exists()


def test_cli_list_mode(tmp_path, capsys):
    main(["--drop-dir", str(tmp_path), "--for-date", "20260909", "--clean"])
    rc = main(["--drop-dir", str(tmp_path), "--list"])
    assert rc == 0
    assert "2 file(s)" in capsys.readouterr().out


def test_cli_rejects_unknown_dirt_mode(tmp_path):
    with pytest.raises(SystemExit):
        main(["--drop-dir", str(tmp_path), "--dirt", "dupes,vortex"])


def test_cli_refuses_filenames_escaping_the_drop_dir(tmp_path):
    from filedrop.generate import _safe_path

    with pytest.raises(ValueError):
        _safe_path(str(tmp_path), "../escape.csv")
    with pytest.raises(ValueError):
        _safe_path(str(tmp_path), "sub/dir/x.csv")
