# Section 6 timing runs: step-by-step guide for teammates

You will start a small Amazon EMR cluster on Hsy's AWS account, run one script that times parts of our project 18 times, download one result file, send it to Hsy, and shut the cluster down. You do not need to know the project or edit any code. Follow every step in order.

- **Time:** about 2 hours in total, of which about 1 hour 30 minutes is the script running by itself (you can leave it).
- **Cost:** about USD 1.50 of Hsy's AWS credit. The cluster costs money every hour it is on, so **always do step 11 (terminate)** at the end, even if something went wrong.
- **What you need:**
  - A Windows laptop with PowerShell (built in), or a Mac with Terminal.
  - Hsy's AWS login details and Hsy's key file (`.pem`).
  - The **bucket name** (Hsy sends it to you; it looks like `inf2006-...-us-east-1-an`). Wherever this guide says `<bucket>`, type that name instead, without the `< >`.

## Why we are doing this
Section 6 of our report must show how long the pipeline takes and whether changing one setting makes it faster. So far each part has been timed only twice. This run times three parts of the pipeline three times each, under two settings:

| Setting | What it is |
|---|---|
| `default` | The cluster exactly as before (Spark splits shuffled data into 200 pieces) |
| `partitions16` | One change: Spark uses 16 pieces instead of 200 (`spark.sql.shuffle.partitions=16`) |

| Part timed | What it does | About |
|---|---|---|
| `task2_price_stages` | Cleans the price data and builds the weekly features | 5 min |
| `task3_validation` | Fits the 8 model configurations on 2013 to 2015 and scores 2016 | 4 min |
| `task4_analytics` | Runs the Task 4 analyses | 4 min |

2 settings x 3 parts x 3 repetitions = 18 timed runs. The script never touches the 2017 test set.

**Do not:** change any other cluster setting, run any other script, or delete anything in S3.

---

## Step 1. Sign in to AWS
1. Sign in with the login Hsy gave you.
   - **If it is an AWS Academy Learner Lab:** open the course, go to **Modules** then **Launch AWS Academy Learner Lab**, click **Start Lab** (top right), wait until the dot next to **AWS** turns **green** (1 to 2 minutes), then click **AWS**. The AWS console opens in a new tab. A lab session lasts about 4 hours; the timer is shown next to **Start Lab**.
   - **If it is a normal AWS account:** sign in at https://console.aws.amazon.com with the details Hsy gave you.
2. In the top-right corner of the console, check the region says **N. Virginia** (`us-east-1`). If not, click it and choose **US East (N. Virginia)**. Everything below must be done in this region.

## Step 2. Save Hsy's key file
Hsy sends you a key file ending in `.pem`. It lets your laptop log in to the cluster. Wherever this guide says `<key>`, type its name without `.pem` (for example, for `abc-kpair.pem` type `abc-kpair`).
1. Save the file in your **Downloads** folder. Keep it private: never post it, forward it or put it in GitHub, and delete it when the project is over.
2. **Windows only:** SSH refuses a key file that other users can read. Open **PowerShell** (Start menu, type PowerShell) and run these three lines:
   ```powershell
   cd $HOME\Downloads
   icacls .\<key>.pem /inheritance:r
   icacls .\<key>.pem /grant:r "$($env:USERNAME):(R)"
   ```
   The last two lines each print `Successfully processed 1 files`.
   **Mac only:** in Terminal: `chmod 400 ~/Downloads/<key>.pem`

## Step 3. Note a public subnet
1. In the console search bar, type **VPC** and open it. In the left menu click **Subnets**.
2. Click any subnet whose **VPC** column shows the default VPC (the one marked `Default VPC: Yes` under **Your VPCs**).
3. On its **Details** tab, check **Auto-assign public IPv4 address** = **Yes**. If it says No, try another subnet in the default VPC.
4. Copy its **Subnet ID** (looks like `subnet-0abc...`) into a notepad.

## Step 4. Create the cluster
1. In the console search bar, type **EMR** and open **Amazon EMR**. In the left menu click **Clusters** (under **EMR on EC2**), then **Create cluster**.
2. Go down the page and set exactly these values; leave everything else as it is:

   | Section | Setting | Value |
   |---|---|---|
   | Name and applications | Name | `inf2006-perf-<yourname>` |
   | | Amazon EMR release | **emr-7.10.0** |
   | | Application bundle | **Custom**, then tick only **Hadoop**, **Hive**, **Spark**, **JupyterEnterpriseGateway**, **Livy** (untick everything else) |
   | | AWS Glue Data Catalog settings | **untick both boxes** |
   | Cluster configuration | Configuration type | **Instance groups** |
   | | Primary | Instance type **m5.xlarge** |
   | | Core | Instance type **m5.xlarge**; under **EBS volumes** click **Add EBS volume**: **gp3**, **100** GiB, IOPS **3000**, Throughput **125** |
   | | Task 1 of 1 | click **Remove instance group** |
   | Cluster scaling and provisioning | Choose an option | **Set cluster size manually** |
   | | Core instance count | **2** |
   | | Provisioning | **On-Demand** (not Spot) |
   | Networking | VPC | the **default VPC** |
   | | Subnet | the subnet ID from step 3 |
   | Cluster termination and node replacement | Termination option | **Automatically terminate cluster after idle time**, idle time **1 hour** |
   | Software settings | Enter configuration | paste the box below |
   | Security configuration and EC2 key pair | Amazon EC2 key pair for SSH | **Hsy's key pair**: the one with the same name as the `.pem` file from step 2 |
   | Identity and Access Management (IAM) roles | Amazon EMR service role | **Choose an existing service role** then `EMR_DefaultRole` |
   | | EC2 instance profile for Amazon EMR | **Choose an existing instance profile** then `EMR_EC2_DefaultRole` (on a Learner Lab it may be called `LabInstanceProfile`; pick that if `EMR_EC2_DefaultRole` is not in the list) |

   Software settings box (copy all of it):
   ```json
   [
     {"Classification": "hdfs-site", "Properties": {"dfs.replication": "2"}},
     {"Classification": "spark-defaults", "Properties": {
       "spark.dynamicAllocation.enabled": "false",
       "spark.executor.instances": "2",
       "spark.executor.cores": "2",
       "spark.executor.memory": "6g",
       "spark.executor.memoryOverhead": "2g",
       "spark.driver.memory": "2g"}}
   ]
   ```
3. Click **Create cluster** (bottom right).
4. Wait until the **Status** at the top says **Waiting** (green). This takes about 10 to 15 minutes; click the refresh button now and then. If it says **Terminated with errors**, see Troubleshooting.

## Step 5. Allow your laptop to connect (one firewall rule)
1. On the cluster page, open the **Properties** tab and scroll to **Network and security**. Under **EC2 security groups (firewall)**, click the link under **Primary node** (named `ElasticMapReduce-master`).
2. In the page that opens, click the security group ID, then the **Inbound rules** tab, then **Edit inbound rules**.
3. Click **Add rule** and set: Type **SSH**, Source **My IP** (it fills in your address). Click **Save rules**.
   Never choose `0.0.0.0/0` (that opens it to the whole internet). If you change Wi-Fi later, add a new rule for the new IP.

## Step 6. Connect to the cluster
1. Back on the cluster page (**Summary** tab), copy the **Primary node public DNS** (it starts with `ec2-` and ends with `.amazonaws.com`).
2. In PowerShell (or Mac Terminal):
   ```powershell
   cd $HOME\Downloads
   ssh -i <key>.pem hadoop@<primary-public-dns>
   ```
   Replace `<key>` with the key name from step 2 and `<primary-public-dns>` with what you copied. The user name is always `hadoop`.
3. The first time it asks `Are you sure you want to continue connecting (yes/no)?`. Type `yes` and press Enter.
4. You are in when you see a big **EMR** logo and a prompt like `[hadoop@ip-172-31-... ~]$`. Every command from now on goes into this window unless the step says otherwise.

## Step 7. Prepare the cluster (about 2 minutes)
Copy and paste these lines one at a time and press Enter after each:
```bash
sudo yum install -y git tmux python3-numpy
git clone https://github.com/jaecodess/quantresearchstuff.git
cd ~/quantresearchstuff
git fetch origin
git switch -c ws/perf/timing-runs --track origin/ws/perf/timing-runs
ls perf/perf_runs.sh
```
The last line must print `perf/perf_runs.sh`.
- If `git switch` says `invalid reference`, the branch has already been merged: run `git switch main && git pull` instead, then `ls perf/perf_runs.sh` again.
- If `ls` says `No such file or directory`, stop and message Hsy.

## Step 8. Start the timing runs (about 1 hour 30 minutes)
1. Start a **tmux** session. tmux keeps the script running even if your laptop sleeps or the connection drops:
   ```bash
   tmux new -s perf
   cd ~/quantresearchstuff
   ```
   The window gets a green bar at the bottom.
2. Start the script. Replace `<bucket>` with the bucket name from Hsy, and use today's date and time as the run name (format `YYYYMMDD-HHMM-perf`):
   ```bash
   bash perf/perf_runs.sh 20261118-1000-perf s3://<bucket>/inf2006
   ```
3. What you should see: first a line saying the environment was recorded, then (on a new cluster) `staging the raw corpus from S3 into HDFS (about 2 minutes, not timed)`, then pairs of lines like:
   ```
   [2026-11-18T02:10:04Z] START default rep 1 task2_price_stages (20261118-1000-perf-default-r1)
   [2026-11-18T02:15:31Z] END   default rep 1 task2_price_stages: 5 min 27 s, exit 0
   ```
   `exit 0` means that run worked. There will be 18 START/END pairs.
4. If it stops straight away with a line starting `STOP:`, read it:
   - `STOP: numpy missing`: run `sudo yum install -y python3-numpy`, then step 8.2 again.
   - `STOP: cannot read s3://...`: the bucket name is wrong (check spelling, no `< >`), then step 8.2 again.
5. **You can now leave it running.** To step away safely, press **Ctrl+b**, let go, then press **d**. This "detaches": the script keeps running on the cluster. You can close PowerShell.
6. To come back later: connect again (step 6), then run `tmux attach -t perf`.
7. Optional, to watch the full detail: inside tmux press **Ctrl+b** then **c** (new window) and run:
   ```bash
   cd ~/quantresearchstuff && tail -f evidence/perf/perf_20261118-1000-perf.log
   ```
   Press **Ctrl+c** to stop watching, **Ctrl+b** then **n** to switch back to the first window.

## Step 9. Check the result
When it finishes (about 1 hour 30 minutes after starting), the first tmux window ends with something like:
```
RESULT: 18 of 18 timed runs succeeded
task                 config         runs       median          min          max  speedup
task2_price_stages   default           3   5 min 20 s   5 min 12 s   5 min 31 s
task2_price_stages   partitions16      3   4 min 50 s   ...
...
[...] finished. Bundle with: tar czf ~/perf_20261118-1000-perf.tar.gz ...
```
- `18 of 18` is the goal. If fewer succeeded, that is still useful evidence: carry on to step 10 and tell Hsy how many failed.
- Take a screenshot of this table for Hsy.

## Step 10. Download the results and send them to Hsy
1. In the cluster window (the prompt that ends in `quantresearchstuff]$`), bundle everything into one file. Use your run name:
   ```bash
   cd ~/quantresearchstuff
   tar czf ~/perf_20261118-1000-perf.tar.gz evidence/perf $(find evidence -name '*20261118-1000-perf*' -not -path 'evidence/perf/*')
   ls -lh ~/perf_20261118-1000-perf.tar.gz
   ```
   `ls` shows the file and its size (a few MB).
2. Open a **new** PowerShell window on your laptop (keep the cluster window open) and run:
   ```powershell
   cd $HOME\Downloads
   scp -i <key>.pem hadoop@<primary-public-dns>:~/perf_20261118-1000-perf.tar.gz .
   ```
   Note the space and the dot at the very end ("copy it here"). The file lands in your Downloads folder.
3. **Backup copy in S3** (do this too, in the cluster window):
   ```bash
   aws s3 cp ~/perf_20261118-1000-perf.tar.gz s3://<bucket>/inf2006/evidence/
   ```
4. Send the `.tar.gz` file and your screenshot to Hsy (Telegram, Teams or email). Hsy commits it to the project.

## Step 11. Terminate the cluster (do not skip)
1. In the AWS console, open **EMR**, then **Clusters**, tick your cluster `inf2006-perf-<yourname>`, and click **Terminate**, then **Terminate** again to confirm.
2. Wait until the status says **Terminating** and then **Terminated** (a few minutes). Data in S3 stays; only the cluster is deleted.
3. On a Learner Lab you can also click **End Lab** afterwards.

---

## Troubleshooting
| What you see | What to do |
|---|---|
| `ssh: connect to host ... Connection timed out` | The firewall rule is missing or your IP changed. Redo step 5 (add SSH, My IP). |
| `Permission denied (publickey)` | Wrong key file or user name. The command must use Hsy's `.pem` from step 2 and `hadoop@`. Check that you chose Hsy's key pair in step 4. |
| `WARNING: UNPROTECTED PRIVATE KEY FILE!` | Redo step 2.2 (the `icacls` lines), then connect again. |
| Cluster status **Terminated with errors** | Open the cluster, read the reason. If it mentions the subnet, pick another subnet with **Auto-assign public IPv4 = Yes** (step 3) and create a new cluster. If it mentions a role or instance profile, use `LabInstanceProfile` / `EMR_DefaultRole`. |
| `git switch` says `invalid reference` | Run `git switch main && git pull`. |
| Connection dropped while the script runs | Nothing is lost. Connect again (step 6) and run `tmux attach -t perf`. |
| A line ends in `exit 1` (or another number) | That one run failed; the script carries on with the next. Let it finish and tell Hsy. |
| The script seems frozen | It prints nothing while a part runs (up to about 5 minutes). Check the detail log (step 8.7). |
| You need to stop everything | Press **Ctrl+c** in the script window, then still do steps 10 and 11. |

## For Hsy, after receiving the file
Upload the `.tar.gz` in the project chat with Claude (or unpack it into the repo and commit `evidence/perf/` with the bucket name redacted). `evidence/perf/summary_<run>.csv` holds the medians, ranges and speedup for report section 6.
