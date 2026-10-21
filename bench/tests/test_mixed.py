import pytest

import mixed

PAGE = """\
# HELP mayu_tasks_settled_total Tasks settled.
# TYPE mayu_tasks_settled_total counter
mayu_tasks_settled_total{queue="a",task="mixed.noop",outcome="success"} 120
mayu_tasks_settled_total{queue="a",task="mixed.noop",outcome="failure"} 7
mayu_tasks_settled_total{outcome="success",queue="b",task="mixed.realistic"} 30
mayu_tasks_received_total{queue="a"} 999
"""


def test_settled_total_sums_successes_only():
    assert mixed.settled_total(PAGE) == 150


def test_settled_total_of_an_empty_page_is_zero():
    assert mixed.settled_total("# HELP nothing yet\n") == 0


def test_group_rate_uses_the_groups_own_final_count():
    series = [(0.0, 0), (1.0, 2), (2.0, 20), (3.0, 60), (4.0, 98), (5.0, 100)]
    # 5 % of 100 is first reached at t=2 (20), 95 % at t=4 (98)
    assert mixed.group_rate(series, 100) == pytest.approx(78 / 2)


def test_group_rate_is_zero_for_a_group_that_did_nothing():
    assert mixed.group_rate([(0.0, 0), (1.0, 0)], 0) == 0.0


def test_group_rate_is_zero_when_there_are_too_few_samples():
    assert mixed.group_rate([(0.0, 0), (1.0, 100)], 100) == 0.0


def test_finish_seconds_is_the_first_sample_at_the_final_count():
    series = [(0.5, 0), (1.5, 40), (2.5, 50), (3.5, 50)]
    assert mixed.finish_seconds(series, 50, default=9.0) == 2.5
    assert mixed.finish_seconds(series, 60, default=9.0) == 9.0
