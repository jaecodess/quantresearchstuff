import check_hygiene as h


def labels(text):
    return [p.split(": ", 1)[1] for p in h.check_text("f.py", text)]


def test_detects_credentials():
    assert labels("key = " + "AK" + "IA" + "A" * 16) == ["AWS access key"]
    assert labels("-----BEGIN " + "RSA PRIVATE KEY-----") == ["private key"]
    assert labels("t = gh" + "p_" + "a" * 36) == ["GitHub token"]


def test_detects_machine_paths():
    assert labels("/Users/" + "alice/data") == ["macOS/Linux home path"]
    assert labels("/home/" + "bob/project") == ["macOS/Linux home path"]
    assert labels("C:\\" + "Users\\alice") == ["Windows user path"]


def test_allows_emr_home_and_markers():
    assert labels("/home/hadoop/job") == []
    assert labels("/Users/" + "alice/x  # hygiene: allow") == []


def test_file_policy():
    assert h.check_file_policy("data/prices.csv", 10)
    assert not h.check_file_policy("manifests/inventory.csv", 10)
    assert h.check_file_policy("raw/gdelt.zip", 10)
    assert h.check_file_policy("config/paths.yaml", 10)
    assert h.check_file_policy("evidence/big.csv", 11 * 1024 * 1024)
    assert not h.check_file_policy("models/pm/data/part.parquet", 20 * 1024 * 1024)


def test_broken_markdown_link(tmp_path):
    (tmp_path / "a.md").write_text("[ok](b.md) [bad](nope.md) [web](https://x.y) [anchor](#h)")
    (tmp_path / "b.md").write_text("hi")
    problems = h.check_links("a.md", (tmp_path / "a.md").read_text(), tmp_path)
    assert len(problems) == 1 and "nope.md" in problems[0]
