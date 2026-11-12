import csv
import json

import timings as t

DFS = """Configured Capacity: 1
DFS Used: 999
Live datanodes (2):

Name: 10.0.0.2:9866 (h2)
DFS Used: 120
Non DFS Used: 6
DFS Remaining: 880

Name: 10.0.0.1:9866 (h1)
DFS Used: 100
Non DFS Used: 5
DFS Remaining: 900
"""
YARN = """Total Nodes:2
ip-10-0-0-1.ec2.internal:8041\tRUNNING\tip-10-0-0-1:8042\t2
Detailed Node Information :
\tAllocated Resources : <memory:8192, vCores:2>
\tResource Utilization by Node : PMem:5000 MB, VMem:5000 MB, VCores:1.0
ip-10-0-0-2.ec2.internal:8041\tRUNNING\tip-10-0-0-2:8042\t1
Detailed Node Information :
\tAllocated Resources : <memory:4096, vCores:1>
\tResource Utilization by Node : PMem:3000 MB, VMem:3000 MB, VCores:1.0
"""
APPS = """Application Report : 
\tApplication-Id : application_1_0001
\tApplication-Name : clean_prices_r
\tApplication-Type : SPARK
\tFinal-State : SUCCEEDED
\tAggregate Resource Allocation : 123456 MB-seconds, 78 vcore-seconds
"""


def make_run(root, run, status="ok", profile="full", config="ref", rep="1", task2=(10, 5), results=None):
    d = root / run
    (d / "pipeline").mkdir(parents=True)
    (d / "model").mkdir()
    (d / "meta.txt").write_text(f"run_id={run}\nprofile={profile}\nconfig={config}\nrep={rep}\nstatus={status}\n")
    with (d / "wrapper_times.csv").open("w") as fh:
        fh.write("task,stage,start_utc,end_utc,seconds,exit_code\n")
        fh.write("prep,dev_input,a,b,99,0\n")
        fh.write(f"task2,_total,a,b,{sum(task2)},0\ntask3,_total,a,b,7,0\n")
    with (d / f"pipeline/stage_times_{run}.csv").open("w") as fh:
        fh.write("stage,start_utc,end_utc,seconds\n")
        fh.write(f"clean_prices,a,b,{task2[0]}\nweekly_features,a,b,{task2[1]}\n")
    (d / f"model/stage_times_{run}.csv").write_text("stage,start_utc,end_utc,seconds\nfit_score_test,a,b,7\n")
    res = results or {"clean": 605887, "application_id": "application_9", "finished_utc": "x"}
    (d / f"pipeline/reconciliation_{run}.json").write_text(json.dumps(res))
    return d


def test_collect_merges_and_replaces_rows(tmp_path):
    table = tmp_path / "timings.csv"
    d = make_run(tmp_path, "r1")
    for _ in range(2):  # rerunning collect replaces the run's rows
        t.main(["collect", "--run-dir", str(d), "--table", str(table)])
    rows = list(csv.DictReader(table.open()))
    assert len(rows) == 6
    assert {(r["task"], r["stage"]) for r in rows} >= {("task2", "clean_prices"), ("task3", "fit_score_test")}
    assert all(r["profile"] == "full" and r["status"] == "ok" for r in rows)


def test_summary_median_range_and_failures(tmp_path):
    table = tmp_path / "timings.csv"
    for run, t2, status in (
        ("r1", (10, 5), "ok"),
        ("r2", (20, 5), "ok"),
        ("r3", (30, 5), "ok"),
        ("r4", (1, 1), "failed"),
    ):
        t.main(["collect", "--run-dir", str(make_run(tmp_path, run, status=status, task2=t2)), "--table", str(table)])
    rows = t.summarise(list(csv.DictReader(table.open())))
    pipe = next(r for r in rows if r["level"] == "pipeline")
    assert (pipe["runs"], pipe["failed"], pipe["n"]) == (4, 1, 3)
    assert (pipe["median"], pipe["min"], pipe["max"]) == (32.0, 22.0, 42.0)  # task2 + task3, prep excluded
    clean = next(r for r in rows if r["stage"] == "clean_prices")
    assert (clean["median"], clean["min"], clean["max"]) == (20.0, 10.0, 30.0)


def test_resources_peaks_without_hostnames(tmp_path):
    d = make_run(tmp_path, "r1")
    raw = d / "raw"
    raw.mkdir()
    (raw / "dfs_report.txt").write_text(
        f"### sample 1\n{DFS}### sample 2\n{DFS.replace('DFS Used: 120', 'DFS Used: 500')}"
    )
    (raw / "yarn_nodes.txt").write_text(
        f"### sample 1\n{YARN}### sample 2\n{YARN.replace('memory:4096', 'memory:6000')}"
    )
    (raw / "yarn_apps.txt").write_text(APPS)
    t.main(["resources", "--run-dir", str(d)])
    text = (d / "resources_peak.csv").read_text()
    assert "ip-10" not in text and "10.0.0" not in text
    peaks = list(csv.DictReader(text.splitlines()))
    assert [p["node"] for p in peaks] == ["node1", "node2"]
    assert peaks[1]["peak_dfs_used_bytes"] == "500" and peaks[1]["min_dfs_remaining_bytes"] == "880"
    assert peaks[0]["peak_alloc_mb"] == "8192" and peaks[1]["peak_alloc_mb"] == "6000"
    assert peaks[0]["samples"] == "2"
    apps = list(csv.DictReader((d / "apps.csv").open()))
    assert apps[0]["mb_seconds"] == "123456" and apps[0]["final_state"] == "SUCCEEDED"


def test_check_ignores_ids_and_times_but_catches_result_changes(tmp_path):
    a = make_run(tmp_path, "a")
    same = make_run(tmp_path, "b", results={"clean": 605887, "application_id": "application_1", "finished_utc": "y"})
    diff = make_run(tmp_path, "c", results={"clean": 605890})
    assert t.main(["check", "--run-dir", str(a), "--against", str(same)]) == 0
    assert t.main(["check", "--run-dir", str(a), "--against", str(diff)]) == 1
