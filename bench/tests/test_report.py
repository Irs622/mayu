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


def mixed_rec(layout, workload, rates, shares=(40.0, 30.0, 30.0), pss=(50.0, 300.0, 280.0)):
    names = ("3.14t thread", "3.14 process", "3.13 process")
    groups = [{"name": n, "python": p, "mode": n.split()[1], "layout": "L", "tasks": 10, "share": sh, "rate": r,
               "finish_seconds": 1.0, "peak_pss_mb": ps, "peak_rss_mb": ps * 2}
              for n, p, r, sh, ps in zip(names, ("3.14.7t", "3.14.8", "3.13.15"), rates, shares, pss)]
    return {"layout": layout, "workload": workload, "n": 100, "executed": 100, "concurrency": 16,
            "rate": sum(rates), "wall_seconds": 1.0, "groups": groups}


def mixed_section(md):
    return md.split("## Mixed Python versions", 1)[1]


def test_no_mixed_section_without_mixed_records():
    assert "Mixed Python versions" not in report.render([rec("rq", "noop", 50)], MACHINE)


def test_mixed_section_follows_memory_section_with_medians_per_layout():
    records = [rec("mayu-thread", "realistic", 1000), rec("celery", "realistic", 900)]
    mixed = [mixed_rec("shared", "noop", (r, 2 * r, 3 * r)) for r in (100, 200, 900)]
    mixed += [mixed_rec("routed", "noop", (500, 500, 500)), mixed_rec("routed", "realistic", (1, 2, 3))]
    md = report.render(records, MACHINE, mixed)
    assert md.index("## Memory at equal concurrency") < md.index("## Mixed Python versions")
    shared, routed = mixed_section(md).split("### Queue per group")
    assert "| noop | 200 (40 %) | 400 (30 %) | 600 (30 %) | 1200 |" in shared
    assert "| peak PSS / RSS MB, noop | 50 / 100 | 300 / 600 | 280 / 560 | |" in shared
    assert "| realistic |" not in shared                  # no realistic runs for this layout
    assert "| noop | 500 (40 %) | 500 (30 %) | 500 (30 %) | 1500 |" in routed
    assert "| realistic | 1 (40 %) | 2 (30 %) | 3 (30 %) | 6 |" in routed


def test_mixed_section_skips_a_group_that_did_not_run():
    run_ = mixed_rec("routed", "noop", (500, 500, 500))
    run_["groups"].pop()                                 # the 3.13 venv was missing
    section = mixed_section(report.render([rec("rq", "noop", 50)], MACHINE, [run_]))
    assert "3.14t thread" in section and "3.14 process" in section
    assert "3.13 process" not in section
