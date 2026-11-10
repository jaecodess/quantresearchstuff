# Adding SEC EDGAR and Wikinews to the raw corpus (S3 and HDFS)

The corpus must also hold the two sources we do not analyse. They are acquired, checked and stored unchanged, with a manifest like the price and GDELT files. Nothing cleans or reads them later.

| Source | What is stored | Size | Check |
|---|---|---|---|
| SEC EDGAR | The 311 filings in the course inventory (156 10-K, 155 10-Q, 303 companies, 2013-2017), one complete-submission `.txt` each. The list is [`config/sec_filing_inventory.csv`](../../config/sec_filing_inventory.csv), converted from the course's `SEC_Filing_Inventory.xlsx`. | about 2.78 GB | each file must contain its own accession number; SHA-256 recorded |
| Wikinews | The full history of the English Wikinews dump of 2026-10-01. Wikimedia splits it into two parts (`p1p481753`, `p481754p3096188`); the two `.bz2` files are the brief's "two history dumps, about 662 MB", and the same two parts in `.7z` are kept as well | 662 MB (`.bz2`) + 284 MB (`.7z`) | MD5 and size from the dump's own `dumpstatus.json` (saved as provenance) |

S3 layout (same in HDFS under `/inf2006/raw/`):
```
inf2006/raw/sec/edgar-complete-submission/acquired=<date>/<cik>_<accession>.txt
inf2006/raw/wikinews/enwikinews-20261001/acquired=<date>/<dump file>
```

Takes about 30 minutes including cluster start.

**Done on 2026-11-10** (evidence: [`manifests/sec_acquired=2026-11-10.csv`](../../manifests/sec_acquired=2026-11-10.csv), [`manifests/wikinews_acquired=2026-11-10.csv`](../../manifests/wikinews_acquired=2026-11-10.csv), [`evidence/corpus/inventory_20261110-1300-aux.json`](../../evidence/corpus/inventory_20261110-1300-aux.json)): SEC 311 / 311 filings, 2,777,408,405 bytes; Wikinews 4 / 4 files, 946,012,974 bytes; all four sources in HDFS, 1,047 / 1,047 files match their manifests. Cost about USD 0.50.

## G0. Cluster and code
Start a cluster and connect as in Task 1 A3 and A4. At the `[hadoop@ip-... ~]$` prompt:
```bash
sudo yum install -y git tmux
git clone https://github.com/jaecodess/quantresearchstuff.git   # skip if the folder already exists
cd ~/quantresearchstuff
git fetch origin
git switch -c ws/corp/aux-sources --track origin/ws/corp/aux-sources
ls config/sec_filing_inventory.csv
tmux new -s aux
cd ~/quantresearchstuff
mkdir -p evidence/corpus
```

## G1. Wikinews (about 5 to 15 minutes)
```bash
python3 corpus/acquire.py wikinews --s3-raw s3://<bucket>/inf2006/raw 2>&1 | tee evidence/corpus/acquire_wikinews.log
```
It first prints the four file names and expected sizes (two parts, each as `.bz2` and `.7z`), and ends with `files ok: 4/4`. If it stops with `dump jobs not complete or not found`, send me the message.

## G2. SEC EDGAR (about 5 to 10 minutes)
SEC blocks downloads that don't say who is asking. Put your own name and email in the quotes (it is sent to SEC only and is not saved in the repo):
```bash
python3 corpus/acquire.py sec --s3-raw s3://<bucket>/inf2006/raw --user-agent "Your Name your.email@example.com" 2>&1 | tee evidence/corpus/acquire_sec.log
```
Progress prints every 25 files. It must end with `files ok: 311/311`. Lines starting `NOT OK` name any filing that failed (for example `unavailable_404`); send me those if any appear.

## G3. Check S3
```bash
aws s3 ls --recursive --summarize s3://<bucket>/inf2006/raw/sec/ | tail -2
aws s3 ls --recursive --summarize s3://<bucket>/inf2006/raw/wikinews/ | tail -2
```
SEC: `Total Objects: 312` (311 filings plus the inventory) and about 2.7 to 2.8 GB. Wikinews: `Total Objects: 5` (four dump files plus `dumpstatus.json`) and about 0.95 GB.

## G4. Copy the full corpus into HDFS and verify (about 5 minutes)
The brief wants the corpus in HDFS, so stage all four sources and check them against the manifests:
```bash
bash corpus/stage_to_hdfs.sh s3://<bucket>/inf2006/raw 20261110-1300-aux
hdfs dfs -ls /inf2006/raw
```
The last command must list `gdelt`, `prices`, `sec` and `wikinews`. The verifier prints a summary: `files_ok` must equal `files_checked`, and every source under `per_source` (including `sec_edgar_filing` with 311 files and `wikinews_history` with 4) must show `"problems": 0`.

## G5. Bring the evidence back and terminate
```bash
tar czf ~/aux_sources_evidence.tar.gz manifests evidence/corpus
```
Download with `scp` from a new PowerShell window in your Downloads folder (note the space and dot at the end):
```powershell
scp -i <key>.pem hadoop@<primary-public-dns>:~/aux_sources_evidence.tar.gz .
```
Upload the `.tar.gz` here, then terminate the cluster. I will redact the bucket name, commit the manifests and evidence, and update the inventory and section 2.

Later sessions stage SEC and Wikinews into HDFS automatically (G4 copies everything under `raw/`), about 3.4 GB more than before. That is well within the 100 GiB of HDFS and adds about a minute.
