"""merge(stats(A), stats(B)) == stats(A ∪ B) for random splits (Chan's parallel algorithm)."""
import numpy as np
import pandas as pd

from core.aggregates import MAX, MEAN, M2, MIN, N, TMAX, TMIN, empty_vec, grouped_stats, merge_stats_frames, merge_vec, vec_from_values


def test_merge_vec_equals_whole_random_splits():
    rng = np.random.default_rng(0)
    x = rng.normal(25, 8, 10_000)
    x[rng.random(x.size) < 0.01] = np.nan
    t = np.arange(x.size) * 3600.0
    whole = vec_from_values(x, t)
    for _ in range(20):
        cuts = np.sort(rng.choice(np.arange(1, x.size), rng.integers(1, 8), replace=False))
        acc = empty_vec(1)
        for a, b in zip(np.r_[0, cuts], np.r_[cuts, x.size]):
            acc = merge_vec(acc, vec_from_values(x[a:b], t[a:b]))
        assert acc[0, N] == whole[0, N]
        assert np.isclose(acc[0, MEAN], whole[0, MEAN], rtol=1e-12)
        assert np.isclose(acc[0, M2], whole[0, M2], rtol=1e-9)
        assert acc[0, MIN] == whole[0, MIN] and acc[0, MAX] == whole[0, MAX]
        assert acc[0, TMIN] == whole[0, TMIN] and acc[0, TMAX] == whole[0, TMAX]


def test_merge_is_commutative_and_handles_empty():
    a = vec_from_values(np.array([1.0, 2.0, 3.0]), np.array([0.0, 1.0, 2.0]))
    b = vec_from_values(np.array([10.0, -4.0]), np.array([5.0, 6.0]))
    ab, ba = merge_vec(a, b), merge_vec(b, a)
    assert np.allclose(ab, ba, equal_nan=True)
    assert np.allclose(merge_vec(empty_vec(1), a), a, equal_nan=True)
    std = np.sqrt(ab[0, M2] / ab[0, N])
    assert np.isclose(std, np.std([1, 2, 3, 10, -4]))


def test_min_tie_break_is_earliest_time():
    a = vec_from_values(np.array([5.0, 1.0]), np.array([10.0, 20.0]))
    b = vec_from_values(np.array([1.0, 7.0]), np.array([3.0, 4.0]))
    assert merge_vec(a, b)[0, TMIN] == 3.0 and merge_vec(b, a)[0, TMIN] == 3.0


def test_merge_stats_frames_matches_direct():
    rng = np.random.default_rng(1)
    ts = pd.date_range("2000-01-01", periods=5000, freq="h")
    df = pd.DataFrame({"timestamp": ts, "x": rng.normal(size=ts.size).round(1), "month": ts.month})
    direct = grouped_stats(df, "timestamp", ["x"], "month")
    parts = [grouped_stats(p, "timestamp", ["x"], "month") for p in (df.iloc[i::4] for i in range(4))]
    merged = merge_stats_frames(pd.concat(parts).assign(station_id="S", level="month"),
                                ["station_id", "level", "period", "variable"])
    m = direct.merge(merged, on=["period", "variable"], suffixes=("", "_m"))
    assert len(m) == len(direct)
    assert (m["count"] == m["count_m"]).all()
    for c in ("mean", "m2", "min", "max"):
        assert np.allclose(m[c], m[f"{c}_m"], rtol=1e-9)
    assert (m["min_time"] == m["min_time_m"]).all() and (m["max_time"] == m["max_time_m"]).all()
