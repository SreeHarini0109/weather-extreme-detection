import numpy as np
from scipy import stats

from core.trends import _longest_run, mann_kendall, ols_slope, sens_slope


def test_sens_and_ols_exact_on_linear():
    x = np.arange(1995, 2025, dtype=float)
    y = 0.03 * (x - 1995) + 20
    assert np.isclose(sens_slope(x, y), 0.03)
    b, r2 = ols_slope(x, y)
    assert np.isclose(b, 0.03) and np.isclose(r2, 1.0)


def test_mann_kendall_detects_trend_and_not_noise():
    rng = np.random.default_rng(3)
    x = np.arange(30)
    _, tau, z, p = mann_kendall(0.05 * x + rng.normal(0, 0.3, 30))
    assert p < 0.05 and z > 0 and tau > 0
    ps = [mann_kendall(rng.normal(size=30))[3] for _ in range(200)]
    assert 0.0 < np.mean(np.array(ps) < 0.05) < 0.12      # ~5 % false positives


def test_mann_kendall_tau_matches_scipy_without_ties():
    rng = np.random.default_rng(4)
    y = rng.normal(size=25)
    _, tau, _, p = mann_kendall(y)
    t_ref = stats.kendalltau(np.arange(25), y)
    assert np.isclose(tau, t_ref.statistic)
    assert abs(p - t_ref.pvalue) < 0.02    # normal approx + continuity correction vs exact


def test_longest_run():
    assert _longest_run(np.array([0, 1, 1, 0, 1, 1, 1, 0], bool)) == 3
    assert _longest_run(np.zeros(5, bool)) == 0
