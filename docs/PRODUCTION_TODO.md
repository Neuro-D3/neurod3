# Production to-do: paper-mapping throughput and infrastructure

Priority work before the pipeline goes to production. Goal: a full paper-mapping
backfill of one archive (primary papers, citing papers, full text, citation
contexts) in **2–3 hours**, without starving the Airflow UI or the API.

Numbers below were measured on staging on 2026-09-25, during the first run with
`max_citing_papers_per_primary = 2000` across all four archives.

## What we measured

| | Staging, 2026-09-25 |
|---|---|
| Airflow VM | e2-standard-2 (2 vCPU, 8 GB, no swap), LocalExecutor, tasks run inside the scheduler container |
| Pool | `paper_mapping_api_pool`, 4–5 slots, shared by all four mapping DAGs |
| Database | Cloud SQL `db-f1-micro` (shared core, 0.6 GB), serves the API, Airflow metadata and the DAG writes; `dag_data` is only 39 MB |
| Primary-paper phase (resolve) | Fine: CRCNS 9 min, SPARC 11 min, OpenNeuro 21 min, DANDI 31 min |
| Citation phase, CRCNS | 1 of 6 batches finished in **2h21m** (27 primary papers, 1,015 citing papers). After 2h40m only 31 of 137 primary papers had been reached. Projected 6–9 more hours, then context extraction |
| Heaviest single primary | `cai-1` → 10.1038/nature12354: 1,999 citing papers, **2h49m** in one task |
| Throughput | ~50–60 citing papers/min across 5 tasks (~7 s per citing paper per task) |
| Per citing paper (one task) | p50 3.3 s, p75 9.3 s, p90 16.7 s, p99 33.5 s |
| By full-text outcome | Europe PMC / PMC hit ≈ 2.5 s; publisher HTML ≈ 8 s; bioRxiv via headless Chrome ≈ 9 s; **no text found ≈ 13.6 s** (2,135 of ~7,000 citing papers) |
| OpenAlex use | Citation listing is cheap: 101 requests for 1,015 citing papers (200 per page). The resolve phase used ~5.1k (OpenNeuro 2,970, DANDI 1,597, SPARC 370, CRCNS 178). The daily 10k budget hit 0 by 18:30 UTC; ~4–5k of it is not in task telemetry |
| VM load | Load average 5–9 on 2 vCPU; scheduler container 140–165% CPU, 3.6 GB; headless Chrome renderers at 20–27% CPU each; 1.7–2 GB free, no swap |
| Side effects | Airflow UI slow; API `/api/paper-mapping/summary` 2.5 s, `/api/datasets` ~1 s |
| Local text cache | `/opt/airflow/output` on the VM boot disk: 860 MB for CRCNS mapping + 498 MB paper-text-fetcher cache |

### Where the time goes

- **Full text is fetched inline, one citing paper at a time.** Listing citing
  papers takes seconds. Each citing paper's full text is then downloaded
  serially inside the citation task. Up to seven sources are tried, including
  headless Chrome.
- **Failures are the slowest case.** A paper with no open text tries every
  source before giving up, averaging 13.6 s. Nothing records the miss, so the
  next run pays the same cost again.
- **Work is split by dataset count, not by work.** 25 datasets per batch, so
  one heavily cited primary (2,000 citing papers) pins one task for hours while
  the other slots finish.
- **Context extraction waits for every citation batch** of the DAG run before
  it starts.
- **Everything shares 2 vCPU,** so the scheduler, DAG processor, UI server and
  Chrome all compete.

## To do

### P0: before production

1. **Move full-text fetching out of the citation task.**
   - Citation tasks only list citing papers and store the edges: minutes per
     archive, about 1 OpenAlex request per 200 citing papers.
   - A separate text stage fetches full text for papers that don't have it yet.
   - Papers are deduplicated across archives, since `papers` is shared, so a
     paper cited by two datasets is fetched once.

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

4. **Remember failed full-text lookups.**
   - Store "unavailable" with the reason and a retry-after date (for example 30
     days), and skip those papers until then.
   - This removes the 13.6 s cost from every later run. It was about 30% of
     citing papers today.

5. **Store paper full text in a persistent shared store (GCS bucket).**
   - Today the text cache lives on the VM boot disk (`/opt/airflow/output`,
     about 1.4 GB after one partial run). A VM rebuild loses it, and
     `papers.fulltext_cache_key` would then point at files that no longer
     exist.
   - Put the text in a bucket, keyed by normalised DOI, read and written by
     both mapping and classification.
   - That also lets a separate backfill VM (item 8) and the regular VM share
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

### P1: infrastructure

8. **A separate large VM for the backfill, on a schedule (for example weekends).**
   - A `backfill` Airflow queue served by a worker on a large VM (for example a
     16-vCPU high-CPU machine, spot if acceptable).
   - A GCE instance schedule starts it on Saturday and stops it when the queue
     is empty or on Sunday night.
   - The regular VM keeps the UI, the scheduler and daily incremental runs.
   - Target: at about 7 s per citing paper, 20k citing papers is about 40
     worker-hours. With 20–30 concurrent fetchers that's about 1.5–2 hours,
     before the savings from items 4 and 7.
   - Price the machine type and spot availability before choosing.

9. **Separate task execution from the scheduler.**
   - Move from LocalExecutor, where tasks run inside the scheduler container,
     to CeleryExecutor or a dedicated worker container with CPU and memory
     limits. Heavy tasks then can't slow the UI or the scheduler.

10. **Pools per resource instead of one mapping pool.**
    - `openalex_pool`, `fulltext_pool`, `chrome_pool` (and the existing
      classification pool), each sized to that resource's limit.
    - Set their sizes in config so they survive a VM reboot.
      `PAPER_MAPPING_API_POOL_SLOTS` currently resets to 2 at start-up.

11. **Upgrade Cloud SQL.**
    - Move `db-f1-micro` to at least `db-g1-small`, or `db-custom-1-3840` for
      production.
    - The database is small (39 MB), so the problem is CPU and memory, not
      storage.
    - Consider separating Airflow's metadata database from `dag_data`.

12. **Memory safety on the Airflow VM.**
    - Add a 2–4 GB swap file (startup script).
    - Set container memory limits.
    - Alert when memory is above 85%.

### P2: user-facing and visibility

13. **Frontend production build.** Staging serves the React development server
    (a 3 MB unminified `bundle.js`).
14. **API latency.**
    - `/api/paper-mapping/summary` takes 2.5 s: cache it or use a materialised
      view.
    - Consider `min_instance_count = 1` on the API to avoid cold starts.
15. **Progress and metrics for long tasks.**
    - Progress logging is done (`utils/batch_progress.py`).
    - Still to do: time per source for full-text fetches, and a dashboard for
      citing papers per minute, the OpenAlex budget and the text hit rate.
