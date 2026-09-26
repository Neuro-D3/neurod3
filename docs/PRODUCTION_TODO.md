# Production to-do: paper-mapping throughput and infrastructure

Priority work before the pipeline goes to production. Goal: a full paper-mapping
backfill of one archive (primary papers, citing papers, full text, citation
contexts) in **2–3 hours**, without starving the Airflow UI or the API.

Numbers below were measured on staging on 2026-09-25, during the first run with
`max_citing_papers_per_primary = 2000` across all four archives. The DANDI,
OpenNeuro and SPARC runs were paused after their primary-paper phase. CRCNS was
stopped at 20:39 UTC after 4h16m, with about 7.5k citing papers stored, up from
726.

## Current architecture

### Paper-mapping DAG (one per archive: `dandi_`, `openneuro_`, `crcns_`, `sparc_paper_mapping`)

```mermaid
flowchart LR
  A[create tables] --> B[fetch dataset ids] --> C["build_batches<br/>25 datasets per batch"]
  C --> D["resolve_and_persist_batch × N<br/>pool: paper_mapping_api_pool<br/>find each dataset's primary papers<br/>(DataCite, OpenAlex, Crossref)"]
  D --> E["fetch_and_persist_citations_batch × N<br/>pool: paper_mapping_api_pool<br/>list citing papers (OpenAlex, 200/page)<br/>then, one at a time, download each<br/>citing paper's full text"]
  E --> F["extract_and_persist_citation_contexts_batch × N<br/>find citation sentences in the cached text"]
  F --> G[summarize_run]
```

- Every stage is a mapped task over the same batches, and **each stage waits for
  every batch of the stage before it**. One slow citation batch holds back
  context extraction for the whole archive.
- Full text comes from `paper-text-fetcher`, which tries in turn: Europe PMC,
  NCBI PMC, Crossref, Unpaywall, publisher HTML, and headless Chrome (bioRxiv,
  PMC). The result, text or "no text", is written as a JSON file to the
  `airflow-output` Docker volume. `papers.fulltext_cache_key` points at that
  file.
- The citation limit (`max_citing_papers_per_primary`) is a run parameter only.
  It is not stored with the dataset (see item 8).

### Staging infrastructure

```mermaid
flowchart LR
  subgraph VM["Airflow VM · e2-standard-2 · 2 vCPU / 8 GB, no swap"]
    UI[api-server / UI] 
    S["scheduler + LocalExecutor<br/>(every task runs here,<br/>incl. headless Chrome)"]
    P[dag-processor]
    T[triggerer]
    V[("airflow-output volume<br/>paper text cache, boot disk")]
    X[cloudsql-proxy]
  end
  SQL[("Cloud SQL · db-f1-micro<br/>airflow + dag_data")]
  API[Cloud Run: API] --> SQL
  FE[Cloud Run: frontend<br/>React dev server] --> API
  X --> SQL
  S --> V
  S -->|OpenAlex, Europe PMC, PMC,<br/>Crossref, Unpaywall, publishers| NET((internet))
```

## What we measured

| | Staging, 2026-09-25 |
|---|---|
| Airflow VM | e2-standard-2 (2 vCPU, 8 GB, no swap), LocalExecutor, tasks run inside the scheduler container |
| Pool | `paper_mapping_api_pool`, 4–5 slots, shared by all four mapping DAGs |
| Database | Cloud SQL `db-f1-micro` (shared core, 0.6 GB), serving the API, Airflow metadata and the DAG writes. **Memory at 100% all day**, CPU about 18%. `dag_data` is only 39 MB |
| Primary-paper phase (resolve) | Fine: CRCNS 9 min, SPARC 11 min, OpenNeuro 21 min, DANDI 31 min |
| Citation phase, CRCNS | 1 of 6 batches finished in **2h21m** (27 primary papers, 1,015 citing papers). After 2h40m only 31 of 137 primary papers had been reached; projected at 6–9 more hours, then context extraction |
| Heaviest single primary | `cai-1` → 10.1038/nature12354: 1,999 citing papers, **2h49m** in one task |
| Throughput | ~50–60 citing papers/min across 5 tasks, about 7 s per citing paper per task |
| Per citing paper (one task) | First visit: median 4.0 s, average 7.9 s (p90 16.7 s, p99 33.5 s). Repeat visit (cache hit): median 0.26 s |
| By full-text outcome | Europe PMC / PMC hit ≈ 2.5 s; publisher HTML ≈ 8 s; bioRxiv via headless Chrome ≈ 9 s; **no text found ≈ 13.6 s** (2,135 of ~7,000 citing papers) |
| OpenAlex use | Citation listing is cheap: 101 requests for 1,015 citing papers (200 per page). The resolve phase used ~5.1k (OpenNeuro 2,970, DANDI 1,597, SPARC 370, CRCNS 178). The daily 10k budget hit 0 by 18:30 UTC; ~4–5k of it is not in task telemetry |
| VM load | CPU 76% average while mapping ran (peak 87%); load average 5–9 on 2 vCPU; memory 56% average (peak 64%); scheduler container 140–165% CPU, 3.6 GB; headless Chrome renderers at 20–27% CPU each |
| Side effects | Airflow UI slow; API `/api/paper-mapping/summary` 2.5 s, `/api/datasets` ~1 s |
| Local text cache | `airflow-output` volume on the VM boot disk: 860 MB of CRCNS mapping text + 498 MB paper-text-fetcher cache |

### Ops Agent / Cloud Monitoring, 2026-09-25 (UTC)

Charts are made from Cloud Monitoring data with
[`images/production-todo/render_metrics.py`](images/production-todo/render_metrics.py),
5-minute means. The early-morning spike is the VM restarting after the resize to
e2-standard-2.

![Airflow VM CPU](images/production-todo/vm-cpu.svg)

![Airflow VM load average](images/production-todo/vm-load.svg)

![Airflow VM memory](images/production-todo/vm-memory.svg)

![Cloud SQL CPU and memory](images/production-todo/cloudsql.svg)

- **VM:** CPU rose from about 20% to 75–87%, and load average stayed above 2
  (both cores busy) for the whole citation phase. It dropped straight back when
  CRCNS was stopped. Memory climbed steadily as the long-running tasks grew, to
  64%.
- **Cloud SQL:** memory is pinned at 100% even at idle, so every query competes
  for cache. This is the most likely cause of the slow API and UI, more than
  CPU.

### Where the time goes

- **Full text is fetched inline, one citing paper at a time.** Listing citing
  papers takes seconds. Each citing paper's full text is then downloaded
  serially inside the citation task, trying up to seven sources, including
  headless Chrome.
- **Failures are the slowest case.** A paper with no open text tries every
  source before giving up, averaging 13.6 s. The miss is recorded (a cache key
  is written either way), so later runs skip it, but it is never retried.
- **Duplicates are mostly caught.** 21% of visits were to a citing paper
  already seen by another dataset or primary (7,374 edges, 5,841 distinct
  citing papers). About 220 of those (~4% of task time) were real double
  downloads, where two tasks reached the same paper before either saved it. The
  worst case was the primary shared by `alm-1` and `ssc-1`: its two tasks ended
  up downloading the same 835 citing papers side by side.
- **Work is split by dataset count, not by work.** 25 datasets per batch, so
  one heavily cited primary (2,000 citing papers) pins one task for hours while
  the other slots finish.
- **Each stage waits for the whole previous stage,** so context extraction
  waits for the slowest citation batch.
- **Everything shares 2 vCPU,** so the scheduler, DAG processor, UI server and
  Chrome all compete.

## To do

### P0: before production

1. **Move full-text fetching out of the citation task.**
   - Citation tasks only list citing papers and store the edges: minutes per
     archive, about 1 OpenAlex request per 200 citing papers.
   - A separate text stage fetches full text for papers that don't have it yet.
   - The text stage works on distinct papers, deduplicated across archives,
     since `papers` is shared. A paper cited by two datasets is fetched once.
   - A task claims each DOI before fetching it, for example with a claim row or
     an advisory lock, so two concurrent tasks never download the same paper.

2. **Fetch full text concurrently, with a limit per source.**
   - Use a thread pool inside each text task (16–32 workers). The work is
     network-bound, so threads are enough.
   - Headless Chrome gets its own small pool (2–4 at once), because each
     renderer uses about a quarter of a core.
   - Give each source its own rate limit (Europe PMC, NCBI with an API key,
     Crossref polite pool, Unpaywall, bioRxiv, publisher sites) instead of one
     global 0.2 s interval.

3. **Split work by citing papers, not datasets.**
   - Build tasks from chunks of about 200 citing papers, so a 2,000-citer
     primary becomes 10 parallel tasks rather than one 3-hour task.

4. **Retry failed full-text lookups on a schedule.**
   - Misses are already cached, but permanently, so a paper that becomes open
     access later is never picked up. They were about 30% of citing papers
     today.
   - Store a retry-after date (for example 30 days) with the reason, and
     re-try only those that are due, in the background at low priority.
   - Stop the hardest sources early: skip the headless-Chrome path for DOIs
     whose publisher has never yielded text.

5. **Store paper full text in a persistent shared store (GCS bucket).**
   - **Today it isn't persisted anywhere safe.** Text lives in the
     `airflow-output` Docker volume on the VM boot disk (`/opt/airflow/output`,
     about 1.4 GB after one partial run). `docker-compose.gce.yml` already marks
     it "ephemeral; TODO move to GCS".
   - It survives restarts, but a VM rebuild loses it, and
     `papers.fulltext_cache_key` would then point at files that no longer exist.
   - Put the text in a bucket, keyed by normalised DOI, read and written by
     both mapping and classification.
   - That also lets a separate backfill VM (item 11) and the regular VM share
     one cache.
   - Add a lifecycle policy, and a one-time upload of the existing cache.

6. **Extract citation contexts per chunk, not after the whole run.**
   - Run context extraction right after a chunk's text is available (or in the
     same task), so contexts don't wait for the slowest batch of the DAG run.

7. **Account for the whole OpenAlex budget.**
   - Find the ~4–5k requests that aren't in task telemetry today (likely
     paper-text-fetcher's own lookups or other runs that day), and count them.
   - Make the resolve phase incremental: skip datasets whose metadata hasn't
     changed since they were last resolved.
   - Check the budget per task, not only at the start of the run.
   - Decide whether production needs a paid OpenAlex tier.

8. **Record how deep each dataset's citations were checked.**
   - Today nothing says whether a primary paper's citing papers were checked up
     to 10, 2,000 or not at all. `max_citing_papers_per_primary` is a run
     parameter and isn't stored.
   - Add fields per (dataset, primary paper) on `<archive>_paper_map`:
     - `citations_checked_at`
     - `citations_cap`: the limit used
     - `citations_available`: OpenAlex's total after the dataset's creation
       date, from `meta.count` on the first page, so no extra request
     - `citations_listed`: how many were stored
   - Then `citations_listed < citations_available` flags truncated papers, a
     later run can resume only those, and the dashboard can show "2,000 of
     8,412 citing papers checked".
   - Also store the run's parameters in `<archive>_paper_resolution_runs`.

9. **Version labels by the find_reuse commit, and relabel when the labels change.**
   - **Why:** staging has 380 labels from the June snippet classifier
     (PRIMARY / SECONDARY / NEITHER / UNKNOWN, `gpt-5.4-nano`) mixed in with
     today's (REUSE / MENTION / NEITHER). Nothing records which classifier
     produced a label, and old labels are only redone after the whole
     never-classified backlog (7,456 pairs).
   - **Anchor on the upstream commit.** `utils/classify_fulltext_reuse.py` is
     vendored from catalystneuro/find_reuse; the commit
     (`3fac8ce14259e467fa40f9d7f0da96f19c1bd1d7`, 2026-09-18) is only in its
     docstring today. Make it `FIND_REUSE_COMMIT` / `FIND_REUSE_COMMIT_DATE`
     constants, with a unit test that the docstring and constants agree.
     Re-vendoring means updating those two lines.
   - **Every classification row stores `reuse_commit`.** New labels always
     carry the latest hash. Backfill: current rows → `3fac8ce`; the June rows →
     `legacy`.
   - **Registry table `classification_label_versions`:** `reuse_commit`,
     `commit_date` (hashes can't be ordered), `first_used_at`, `labels` (JSON
     of the label sets: citing and direct labels, REUSE sub-types,
     modalities), `label_fingerprint`, `labels_changed`, and `reuse_release`
     (empty until find_reuse tags releases). The DAG registers the current
     commit automatically on its first run with it.
   - **Outdated = the labels changed** (decided 2026-09-26). A new commit with
     the same label sets doesn't make older rows outdated. A commit whose
     fingerprint differs flips `labels_changed`, and every row from an earlier
     commit with different labels becomes outdated. A future major find_reuse
     release that cuts new labels is exactly this case.
   - **DAG param `relabel_order`:** `new_first` (default, today's order),
     `outdated_first`, `outdated_only`.
   - **"No full text" on an outdated row replaces the old label** (decided
     2026-09-26). Otherwise ~40% of outdated rows would never lose their old
     label and would be re-picked by every relabel run. Errors and dry runs
     still never replace a real label, and "no full text" never replaces a
     current one.
   - **History table `paper_reuse_classification_history`:** every replaced
     label is copied there, whole (`to_jsonb`), with its `reuse_commit`, in
     the same statement as the upsert. Note: don't use `FOR UPDATE` in that
     CTE; Postgres then hides the row from it when the upsert updates it.
   - **Dashboard:** outdated labels shown as their own bucket ("Older
     classifier", with a breakdown by old label), filterable, and badged in
     the dataset panel with the old commit on hover.
   - **Starting point:** local branch `wip/relabel-order-draft` (e36e2d9) has a
     working draft of the history table, the write guard, `relabel_order` and
     the dashboard bucket, keyed on prompt version instead of the commit. Its
     guard and history were checked against a real database.

### P1: infrastructure

10. **Upgrade Cloud SQL.**
   - Memory is at 100% on `db-f1-micro` (0.6 GB) even at idle. Warm, the
     dashboard's summary query takes ~1.9 s and the dataset list ~2.3 s.
   - **Staging: `db-g1-small`** (shared core, 1.7 GB, ~$26/mo). One
     Terraform variable (`db_tier`). Do this before production work starts.
   - **Production: `db-custom-2-7680`** (2 vCPU, 7.5 GB, ~$100/mo), or
     `db-custom-1-3840` (1 vCPU, 3.75 GB, ~$50/mo) if load testing shows it's
     enough. Dedicated cores avoid shared-core throttling during backfills.
   - The data is small (39 MB); the problem is memory and CPU, not storage.
   - It also fails tasks. On 2026-09-25 a `paper_reuse_classification` batch
     finished and saved its labels, but Airflow took ~8 s to store its XCom.
     The Task SDK re-sent the write and got a duplicate-key 409, and the task
     was marked failed. Until this is fixed, mark such a task success rather
     than clearing it, since clearing re-runs (and re-pays for) its LLM calls.
   - Consider separating Airflow's metadata database from `dag_data`.
   - The change restarts the database for a minute or two.

11. **A separate large VM for the backfill, on a schedule (for example weekends).**
    - A `backfill` Airflow queue served by a worker on a large VM (for example a
      16-vCPU high-CPU machine, spot if acceptable).
    - A GCE instance schedule starts it on Saturday and stops it when the queue
      is empty or on Sunday night.
    - The regular VM keeps the UI, the scheduler and daily incremental runs.
    - Target: at about 7 s per citing paper, 20k citing papers is about 40
      worker-hours. With 20–30 concurrent fetchers that's about 1.5–2 hours.
      Later runs are much shorter, since cached papers take about 0.3 s.
    - Needs items 5 (shared text store) and 12 (a worker outside the scheduler).

12. **Separate task execution from the scheduler.**
    - Move from LocalExecutor, where tasks run inside the scheduler container,
      to CeleryExecutor or a dedicated worker container with CPU and memory
      limits. Heavy tasks then can't slow the UI or the scheduler.

13. **Pools per resource instead of one mapping pool.**
    - `openalex_pool`, `fulltext_pool`, `chrome_pool` (and the existing
      classification pool), each sized to that resource's limit.
    - Set their sizes in config so they survive a VM reboot.
      `PAPER_MAPPING_API_POOL_SLOTS` currently resets to 2 at start-up.

14. **Memory safety on the Airflow VM.**
    - Add a 2–4 GB swap file (startup script).
    - Set container memory limits.
    - Alert when memory is above 85%.

15. **Upgrade Airflow 3.1.5 → 3.3.x.**
    - 3.3.2 is current (2026-09-17). 3.2.0 added UI performance work and ~42×
      faster rendered-field cleanup for DAGs with many mapped tasks, which
      matches ours. 3.2.0 also moved to SQLAlchemy 2.0, so check custom SQL
      and plugins.
    - None of the 3.2–3.3 notes mention scheduler or pool changes, so this
      doesn't replace items 1–3, 12 and 13.
    - Soak it on staging with `stack_integration_test` before production.

16. **Fail the deploy pipeline when the stack integration test fails.**
    - Today `stack_integration_test` is triggered by hand after a deploy.
    - Target: after deploying, the GitHub Actions workflow triggers it through
      the Airflow REST API, polls until the run finishes (it takes ~5–10 min),
      and fails the job, naming the failed steps from the report, when the
      run fails. A red deploy then means "pushed, but the stack isn't wired".
    - Needs: the runner can reach the Airflow API (the VM's HTTPS endpoint) and
      has credentials for it (an Airflow user for CI, its password in Secret
      Manager, read by the deployer service account through Workload Identity).
    - Unpause the DAGs the test drives before triggering (4 ingestion, 4
      mapping, `paper_reuse_classification`).
    - The VM needs `D3_API_URL`, `D3_FRONTEND_URL` and `D3_FRONTEND_ORIGINS`
      (Cloud Run URLs and `allowed_origins`), written by the startup script
      from Terraform, so the test needs no parameters.

### P2: user-facing and visibility

17. **Frontend production build.**
    - Staging runs `npm start`, the React development server, which compiles
      the app with webpack after the container starts. Cloud Run scales the
      frontend to zero, so every cold start compiles again: about **70 s**
      from instance start to a usable page (Cloud Run logs, 2026-09-26,
      08:28:08 → 08:29:20; same at 07:56, 06:15, 05:29). The port opens
      before the compile ends, so the first visitor waits the whole time. It
      then downloads a 3.2 MB unminified `bundle.js`.
    - Build at image build time (`npm run build`) and serve the static files
      with nginx. Expected cold start ~1–2 s, bundle roughly 0.5–1 MB.
    - A production build bakes `REACT_APP_API_URL` in at build time, so the
      deploy workflow must pass the API URL as a Docker build argument.
    - Turn CPU throttling back on for the frontend (it's `cpu-throttling:
      false` today, so CPU is billed whenever an instance is up); a static
      server doesn't need always-on CPU.
18. **API cold starts and latency.**
    - Keep one API instance warm: `min_instance_count = 1` on the API's Cloud
      Run service (a few dollars a month). Today it scales to zero and the
      first dashboard request waits for Python to start.
    - `/api/paper-mapping/summary` takes 1.9–4.8 s: cache it or use a
      materialised view. Recheck after item 10.
19. **Progress and metrics for long tasks.**
    - Progress logging is done (`utils/batch_progress.py`).
    - Still to do: time per source for full-text fetches, and a dashboard for
      citing papers per minute, the OpenAlex budget and the text hit rate.

## Projected cost

Approximate on-demand list prices for us-west1 as of 2026-09. Confirm in the
[GCP pricing calculator](https://cloud.google.com/products/calculator) before
committing; spot prices vary.

| Component | Staging today | Production proposal | Monthly (≈) |
|---|---|---|---|
| Airflow VM (UI, scheduler, daily runs) | e2-standard-2, ~$49/mo | e2-standard-2, with tasks moved to a worker (item 12). e2-standard-4 if they stay local | $49–98 |
| Backfill VM, weekends only | — | e2-highcpu-16, ~10 h per weekend: ~$0.40/h on-demand, ~$0.12/h spot | $5–17 |
| Cloud SQL | db-f1-micro, ~$8/mo + storage (→ db-g1-small, ~$26) | db-custom-1-3840 (~$50) or db-custom-2-7680 (~$100) | $50–100 |
| Paper text store | boot disk (free, not durable) | GCS Standard, 10–50 GB at ~$0.02/GB | < $1 |
| Boot disk, Cloud Run, Artifact Registry | ~$5–10/mo | same | $5–10 |
| OpenAlex | free key, 10k requests/day | free, or a paid tier if item 7 shows it's needed | TBD |
| API warm instance (item 18) | scales to zero | `min_instance_count = 1` | ~$5 |
| **Total** | **~$65–70/mo** (~$85 with db-g1-small) | lean: **~$115/mo**; comfortable: **~$230/mo** | |
