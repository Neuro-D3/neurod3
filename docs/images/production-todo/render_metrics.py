"""
Fetch staging metrics from Cloud Monitoring and render stdlib-only SVG line charts
for docs/PRODUCTION_TODO.md.

    TOKEN=$(gcloud auth print-access-token) python render_metrics.py 2026-09-25T21:00:00Z docs/images/production-todo

START, the VM instance id and EVENTS are set for the 2026-09-25 staging run; edit
them for another window. Also writes raw.json with the fetched points.
"""
import json, os, sys, urllib.parse, urllib.request
from datetime import datetime

TOKEN = os.environ["TOKEN"]; PROJECT = "neuro-d3-staging"; VM_ID = "8119844806665421049"
START, END = "2026-09-25T09:00:00Z", sys.argv[1]
OUT = sys.argv[2]

def series(filt, reducer=None, aligner="ALIGN_MEAN", period="300s"):
    q = {"filter": filt, "interval.startTime": START, "interval.endTime": END,
         "aggregation.alignmentPeriod": period, "aggregation.perSeriesAligner": aligner}
    if reducer:
        q["aggregation.crossSeriesReducer"] = reducer
    url = f"https://monitoring.googleapis.com/v3/projects/{PROJECT}/timeSeries?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    with urllib.request.urlopen(req) as resp:
        d = json.load(resp)
    out = []
    for ts in d.get("timeSeries", []):
        pts = []
        for p in ts["points"]:
            t = datetime.fromisoformat(p["interval"]["endTime"].replace("Z", "+00:00"))
            v = p["value"].get("doubleValue", p["value"].get("int64Value"))
            pts.append((t, float(v)))
        out.append((ts["metric"].get("labels", {}), sorted(pts)))
    return out

vm = f'resource.labels.instance_id="{VM_ID}"'
data = {
  "cpu": series(f'metric.type="compute.googleapis.com/instance/cpu/utilization" AND {vm}'),
  "mem": series(f'metric.type="agent.googleapis.com/memory/percent_used" AND metric.labels.state="used" AND {vm}'),
  "load": series(f'metric.type="agent.googleapis.com/cpu/load_1m" AND {vm}'),
  "sqlcpu": series('metric.type="cloudsql.googleapis.com/database/cpu/utilization" AND resource.type="cloudsql_database"'),
  "sqlmem": series('metric.type="cloudsql.googleapis.com/database/memory/utilization" AND resource.type="cloudsql_database"'),
}
with open(os.path.join(OUT, "raw.json"), "w") as f:
    json.dump({k: [(l, [(t.isoformat(), v) for t, v in p]) for l, p in s] for k, s in data.items()}, f, indent=0)

EVENTS = [("2026-09-25T16:23:00+00:00", "mapping runs start"),
          ("2026-09-25T17:38:00+00:00", "citation phase"),
          ("2026-09-25T18:30:00+00:00", "OpenAlex budget 0"),
          ("2026-09-25T20:39:47+00:00", "CRCNS stopped")]

def svg(title, lines, ymax, unit, fname, ref=None):
    W, H, L, R, T, B = 760, 300, 56, 16, 40, 44
    t0 = datetime.fromisoformat(START.replace("Z", "+00:00")).timestamp()
    t1 = datetime.fromisoformat(END.replace("Z", "+00:00")).timestamp()
    X = lambda t: L + (t.timestamp() - t0) / (t1 - t0) * (W - L - R)
    Y = lambda v: T + (1 - min(v, ymax) / ymax) * (H - T - B)
    s = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="Helvetica,Arial,sans-serif" font-size="11">',
         f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
         f'<text x="{L}" y="22" font-size="14" font-weight="bold" fill="#1f2328">{title}</text>']
    for i in range(5):
        v = ymax * i / 4; y = Y(v)
        s.append(f'<line x1="{L}" y1="{y:.1f}" x2="{W-R}" y2="{y:.1f}" stroke="#e5e7eb"/>')
        s.append(f'<text x="{L-6}" y="{y+4:.1f}" text-anchor="end" fill="#57606a">{v:g}{unit}</text>')
    h = int(START[11:13])
    while h <= int(END[11:13]):
        t = datetime.fromisoformat(f"2026-09-25T{h:02d}:00:00+00:00"); x = X(t)
        s.append(f'<text x="{x:.1f}" y="{H-24}" text-anchor="middle" fill="#57606a">{h:02d}:00</text>')
        h += 2
    s.append(f'<text x="{(L+W-R)/2}" y="{H-8}" text-anchor="middle" fill="#57606a">UTC, 2026-09-25</text>')
    for i, (ts, label) in enumerate(EVENTS):
        x = X(datetime.fromisoformat(ts))
        s.append(f'<line x1="{x:.1f}" y1="{T}" x2="{x:.1f}" y2="{H-B}" stroke="#9ca3af" stroke-dasharray="4 3"/>')
        anchor, tx = ("end", x - 3) if x > W - 130 else ("start", x + 3)
        s.append(f'<text x="{tx:.1f}" y="{T+10+12*i}" text-anchor="{anchor}" fill="#6b7280" font-size="10">{label}</text>')
    if ref is not None:
        y = Y(ref[0]); s.append(f'<line x1="{L}" y1="{y:.1f}" x2="{W-R}" y2="{y:.1f}" stroke="#dc2626" stroke-dasharray="6 3"/>')
        s.append(f'<text x="{L+4}" y="{y-4:.1f}" fill="#dc2626" font-size="10">{ref[1]}</text>')
    lx = W - R - sum(14 + 7 * len(n) + 18 for n, _, p in lines if p)
    for name, color, pts in lines:
        if not pts: continue
        d = " ".join(f"{'M' if i == 0 else 'L'}{X(t):.1f},{Y(v):.1f}" for i, (t, v) in enumerate(pts))
        s.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2"/>')
        s.append(f'<rect x="{lx}" y="13" width="10" height="3" fill="{color}"/><text x="{lx+14}" y="18" fill="#1f2328">{name}</text>')
        lx += 14 + 7 * len(name) + 18
    s.append("</svg>")
    with open(os.path.join(OUT, fname), "w") as f:
        f.write("\n".join(s))

pts = lambda k, scale=1.0: [(t, v * scale) for t, v in (data[k][0][1] if data[k] else [])]
svg("Airflow VM (e2-standard-2): CPU utilization", [("CPU %", "#2563eb", pts("cpu", 100))], 100, "%", "vm-cpu.svg")
svg("Airflow VM: memory used", [("memory used %", "#7c3aed", pts("mem"))], 100, "%", "vm-memory.svg", ref=(85, "85% alert threshold"))
svg("Airflow VM: load average (1 min) on 2 vCPU", [("load 1m", "#ea580c", pts("load"))], 10, "", "vm-load.svg", ref=(2, "2 = both vCPUs busy"))
svg("Cloud SQL (db-f1-micro): CPU and memory", [("CPU %", "#059669", pts("sqlcpu", 100)), ("memory %", "#db2777", pts("sqlmem", 100))], 100, "%", "cloudsql.svg")
for k in data:
    p = pts(k, 100 if k in ("cpu", "sqlcpu", "sqlmem") else 1)
    if p:
        base = [v for t, v in p if t.hour < 16]; busy = [v for t, v in p if t.hour >= 18]
        print(k, "points", len(p), "baseline avg %.1f" % (sum(base)/len(base) if base else -1), "busy avg %.1f max %.1f" % (sum(busy)/len(busy) if busy else -1, max(busy) if busy else -1))
    else:
        print(k, "NO DATA")
