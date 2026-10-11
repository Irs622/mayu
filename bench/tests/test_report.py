import report


def rec(system, workload, rate, pss=100.0, layout="L"):
    return {"system": system, "workload": workload, "n": 100, "executed": 100, "concurrency": 64,
            "layout": layout, "rate": rate, "rate_kind": "steady", "wall_seconds": 1.0,
            "peak_rss_mb": pss * 2, "peak_pss_mb": pss, "python": "3.14.7", "library_version": "1.0"}


MACHINE = {"cpu": "Test CPU", "cores": 8, "redis": "8.0.0"}


def test_median_of_repeats_and_ratio_to_celery():
    md = report.render([rec("celery", "noop", r) for r in (90, 100, 300)]
                       + [rec("mayu-thread", "noop", r) for r in (400, 500, 600)], MACHINE)
    rows = [line for line in md.splitlines() if line.startswith("| mayu-thread") or line.startswith("| celery")]
    assert rows[0].startswith("| mayu-thread")          # sorted by rate, fastest first
    assert "| 500 |" in rows[0] and "| 5.00× |" in rows[0]
    assert "| 100 |" in rows[1] and "| 1.00× |" in rows[1]


def test_ratio_column_without_celery_is_a_dash():
    md = report.render([rec("rq", "noop", 50)], MACHINE)
    row = next(line for line in md.splitlines() if line.startswith("| rq"))
    assert row.rstrip().endswith("| — |")


def test_memory_section_compares_mayu_thread_and_celery_on_realistic():
    md = report.render([rec("mayu-thread", "realistic", 1000, pss=200.0, layout="2 processes × 32 threads"),
                        rec("celery", "realistic", 900, pss=1600.0, layout="64 prefork processes")], MACHINE)
    section = md.split("## Memory at equal concurrency", 1)[1]
    assert "200" in section and "1600" in section and "8.0×" in section


def test_header_names_the_machine():
    md = report.render([rec("rq", "noop", 50)], MACHINE)
    assert "Test CPU" in md and "8 cores" in md and "Redis 8.0.0" in md
