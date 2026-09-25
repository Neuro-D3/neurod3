#!/usr/bin/env python3
"""
Render a Mermaid .mmd file to PNG with headless Chromium.

The host machines this repo is developed on do not carry Python, Node or
Graphviz, but the Airflow image already ships Playwright + Chromium for
paper-text-fetcher, so diagrams are rendered inside a running container:

    docker compose cp docs/diagrams airflow-scheduler:/tmp/diagrams
    docker compose exec airflow-scheduler python /tmp/diagrams/render_mermaid.py \
        /tmp/diagrams/reuse_classification_flow.mmd /tmp/diagrams/reuse_classification_flow.png
    docker compose cp airflow-scheduler:/tmp/diagrams/reuse_classification_flow.png docs/diagrams/

Mermaid itself is loaded from the jsDelivr CDN, so the container needs
outbound HTTPS. The PNG is captured at 2x device scale for legibility.
"""

import sys
from pathlib import Path

MERMAID_CDN = "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"

HTML = """<!doctype html>
<html><head><meta charset="utf-8">
<style>
  body {{ margin: 0; background: #ffffff; }}
  #wrap {{ display: inline-block; padding: 24px; background: #ffffff; }}
  .mermaid svg {{ font-family: "Segoe UI", Helvetica, Arial, sans-serif; }}
</style>
<script src="{cdn}"></script>
</head><body>
<div id="wrap"><pre class="mermaid">{source}</pre></div>
<script>
  mermaid.initialize({{
    startOnLoad: true,
    theme: "neutral",
    themeVariables: {{ fontSize: "18px" }},
    flowchart: {{ htmlLabels: true, curve: "basis", nodeSpacing: 45, rankSpacing: 60, padding: 12 }},
    securityLevel: "loose",
  }});
</script>
</body></html>
"""


def main(argv):
    if len(argv) != 3:
        print("usage: render_mermaid.py INPUT.mmd OUTPUT.png", file=sys.stderr)
        return 2
    src_path, out_path = Path(argv[1]), Path(argv[2])
    source = src_path.read_text(encoding="utf-8")
    source = source.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html = HTML.format(cdn=MERMAID_CDN, source=source)

    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={"width": 1800, "height": 1200}, device_scale_factor=2)
        page.set_content(html, wait_until="networkidle")
        page.wait_for_selector(".mermaid svg", timeout=60_000)
        page.wait_for_timeout(500)
        # Ask mermaid to parse the raw source so a syntax error is reported as
        # text here instead of as a bomb icon in the PNG.
        parse_error = page.evaluate(
            """async (src) => {
                try { await mermaid.parse(src); return null; }
                catch (e) { return String(e && e.message || e); }
            }""",
            src_path.read_text(encoding="utf-8"),
        )
        if parse_error:
            print(f"mermaid syntax error in {src_path}:\n{parse_error}", file=sys.stderr)
            browser.close()
            return 1
        page.locator("#wrap").screenshot(path=str(out_path), type="png")
        browser.close()
    print(f"wrote {out_path} ({out_path.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
