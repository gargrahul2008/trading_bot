from __future__ import annotations

from intraday_research.universe import ResearchUniverse, load_universe, save_universe


def test_save_and_load_universe_roundtrip(tmp_path) -> None:
    universe = ResearchUniverse(
        name="sample_equities",
        symbols=("NSE:RELIANCE-EQ", "NSE:SBIN-EQ"),
    )

    path = save_universe(universe, tmp_path / "sample_universe.json")
    loaded = load_universe(path)

    assert loaded == universe


def test_load_universe_requires_name_and_symbols(tmp_path) -> None:
    bad_path = tmp_path / "bad_universe.json"
    bad_path.write_text('{"name": "", "symbols": []}')

    try:
        load_universe(bad_path)
    except ValueError as exc:
        assert "Universe file" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid universe file")
