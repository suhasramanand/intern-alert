#!/usr/bin/env python3
"""Fetch Jobright Data Analysis page and parse __NEXT_DATA__ directly. Print jobs."""
import json
import re
import sys
from datetime import datetime, timezone

# Reuse fetch from alert
sys.path.insert(0, ".")
from alert import fetch, JOBRIGHT_DA_URL, JOBRIGHT_MINISITES, parse_jobright_next_data

def parse_all(html):
    """Parse __NEXT_DATA__ and return ALL jobs (no 2hr filter) with posted time."""
    jobs = []
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not m:
        return jobs
    try:
        data = json.loads(m.group(1).strip())
        job_list = (data.get("props") or {}).get("pageProps", {}).get("initialJobs") or []
    except (json.JSONDecodeError, TypeError):
        return jobs
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    for j in job_list:
        if not isinstance(j, dict):
            continue
        jid = j.get("id") or ""
        raw_ms = j.get("postedDate")
        if not jid:
            continue
        try:
            posted_ms = int(raw_ms) if raw_ms is not None else 0
        except (TypeError, ValueError):
            posted_ms = 0
        diff_ms = now_ms - posted_ms
        mins_ago = diff_ms // (60 * 1000)
        if mins_ago < 60:
            posted_str = f"{mins_ago} min ago" if mins_ago == 1 else f"{mins_ago} mins ago"
        else:
            hrs = mins_ago // 60
            posted_str = f"{hrs} hour ago" if hrs == 1 else f"{hrs} hours ago"
        title = (j.get("title") or "")[:200]
        company = (j.get("company") or "")[:100]
        url = (j.get("applyUrl") or "").strip() or f"https://jobright.ai/jobs/info/{jid}"
        jobs.append({
            "id": jid, "title": title, "company": company,
            "url": url.split("?")[0], "posted_str": posted_str, "mins_ago": mins_ago,
            "posted_ms": posted_ms,
        })
    return jobs

if __name__ == "__main__":
    # Fetch all 3: Data Analysis, AI/ML, Business Analyst
    print("Fetching all 3 Jobright minisites (Data Analysis, AI/ML, Business Analyst)...\n")
    combined = []
    seen = set()
    for url in JOBRIGHT_MINISITES:
        name = "data_analysis" if "data_analysis" in url else ("aiml" if "aiml" in url else "business_analyst")
        html = fetch(url)
        jobs = parse_all(html)
        for j in jobs:
            if j["id"] not in seen:
                seen.add(j["id"])
                j["_source"] = name
                combined.append(j)
        print(f"  {name}: {len(jobs)} jobs")

    combined.sort(key=lambda x: -x["posted_ms"])
    print(f"\nCombined (deduped): {len(combined)} jobs. Latest 25:\n")
    print("-" * 80)
    for i, j in enumerate(combined[:25], 1):
        src = j.get("_source", "")
        print(f"{i}. [{src}] {j['title'][:55]} | {j['company']}")
        print(f"   {j['posted_str']}  {j['url']}")
        print()
