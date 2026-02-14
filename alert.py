#!/usr/bin/env python3
"""Hourly DS/Analytics/ML internship alerts from intern-list + jobright. Dedup via seen_ids file."""
import os, re, smtplib, ssl
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.request import Request, urlopen

SEEN_FILE = os.environ.get("SEEN_FILE", "seen_ids.txt")
INTERN_LIST_URL = "https://www.intern-list.com/da-intern-list"
JOBRIGHT_URL = "https://jobright.ai/jobs/data-scientist-intern-jobs-in-united-states"
def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_seen(seen):
    with open(SEEN_FILE, "w") as f:
        f.write("\n".join(sorted(seen)))

def fetch(url, headers=None):
    req = Request(url, headers=headers or {"User-Agent": "Mozilla/5.0"})
    return urlopen(req, timeout=15).read().decode("utf-8", errors="replace")

def parse_intern_list(html, today_utc):
    """Parse all jobs; caller filters by today_utc."""
    jobs, seen_ids = [], set()
    for m in re.finditer(
        r'href="(/da-intern-list/[^"]+)"[^>]*>.*?<p class="jobtitle">([^<]+)</p><p class="blogtag">([^<]+)</p>.*?<p class="companyname_list">([^<]+)</p>',
        html, re.DOTALL
    ):
        path, title, date_str, company = m.groups()
        job_id = path.split("/")[-1]
        if job_id in seen_ids:
            continue
        seen_ids.add(job_id)
        try:
            pt = datetime.strptime(date_str.strip(), "%B %d, %Y").replace(tzinfo=timezone.utc)
        except ValueError:
            pt = None
        jobs.append({
            "id": f"il_{job_id}", "title": title.strip(), "company": company.strip(),
            "url": f"https://www.intern-list.com{path}", "posted": pt, "posted_str": date_str.strip()
        })
    return jobs

def parse_jobright(html):
    jobs = []
    # Match "X hours ago" or "X minutes ago" (<=1hr only), then link with job id
    time_ok = re.compile(r"(\d+)\s*(minute|hour)s?\s+ago", re.I)
    for m in re.finditer(r"(\d+\s*(?:minute|hour)s?\s+ago)[^[]*\[([^\]]+)\][^\]]*\(https://jobright\.ai/jobs/info/([a-f0-9]+)", html):
        time_str, title, jid = m.group(1), m.group(2), m.group(3)
        g = time_ok.match(time_str)
        if not g:
            continue
        n, unit = int(g.group(1)), g.group(2).lower()
        mins = n * 60 if unit == "hour" else n
        if mins > 60:
            continue  # only ≤1hr
        jobs.append({
            "id": f"jr_{jid}", "title": title.strip(), "company": "",
            "url": f"https://jobright.ai/jobs/info/{jid}", "posted": None, "posted_str": time_str
        })
    return jobs

def main():
    seen = load_seen()
    all_jobs = []
    today_utc = datetime.now(timezone.utc).date()
    print(f"Today UTC: {today_utc}")

    try:
        html = fetch(INTERN_LIST_URL)
        parsed = parse_intern_list(html, today_utc)
        # UTC: only jobs dated today
        for j in parsed:
            if j["posted"] and j["posted"].date() != today_utc:
                continue
            new = j["id"] not in seen
            seen.add(j["id"])
            if new:
                all_jobs.append((j, "intern-list"))
        print(f"intern-list: {len(parsed)} parsed, {sum(1 for j in parsed if j['posted'] and j['posted'].date() == today_utc)} today UTC, {len([x for x in all_jobs if x[1]=='intern-list'])} new")
    except Exception as e:
        print(f"intern-list fetch error: {e}")

    try:
        html = fetch(JOBRIGHT_URL)
        jr_parsed = parse_jobright(html)
        for j in jr_parsed:
            new = j["id"] not in seen
            seen.add(j["id"])
            if new:
                all_jobs.append((j, "jobright"))
        print(f"jobright: {len(jr_parsed)} parsed (≤1hr), {len([x for x in all_jobs if x[1]=='jobright'])} new")
    except Exception as e:
        print(f"jobright fetch error: {e}")

    to_send = all_jobs
    # Latest first: jobright by "X min ago" (smaller = newer), then intern-list by date desc
    def sort_key(x):
        j, _ = x
        if j["posted"]:
            return (1, -j["posted"].timestamp())
        return (0, 0)  # jobright first when no posted
    to_send.sort(key=sort_key)
    save_seen(seen)  # always persist so GH cache has a file to save

    # Log to workflow so "Jobs scraped" is visible in Actions run
    print(f"Jobs scraped this run: {len(all_jobs)} total, {len(to_send)} new to send.")
    for j, src in to_send[:20]:  # first 20 in log
        t = j["title"][:60] + ("..." if len(j["title"]) > 60 else "")
        print(f"  [{src}] {t} | {j['posted_str']}")
    if len(to_send) > 20:
        print(f"  ... and {len(to_send) - 20} more (see email).")

    if not to_send:
        return

    body = "DS/Analytics/ML internships (new in last hour, latest first)\n\n"
    for j, src in to_send:
        body += f"- {j['title']} | {j['company']}\n  Posted: {j['posted_str']}\n  {j['url']}\n\n"

    to_addr = os.environ.get("EMAIL_TO")
    from_addr = os.environ.get("EMAIL_FROM") or to_addr
    password = os.environ.get("EMAIL_APP_PASSWORD")
    if not to_addr or not password:
        print("Set EMAIL_TO and EMAIL_APP_PASSWORD. Body:\n" + body)
        return

    # Gmail app password: use no spaces (e.g. ayta ukmc oejh wceg -> aytaukmcoehjwceg)
    if password:
        password = password.replace(" ", "")

    msg = MIMEMultipart()
    msg["Subject"] = f"Intern alert: {len(to_send)} new DS/ML/Analytics internships"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as s:
            s.login(from_addr, password)
            s.sendmail(from_addr, to_addr, msg.as_string())
        print(f"Email sent to {to_addr} ({len(to_send)} jobs).")
    except Exception as e:
        print(f"Email failed: {e}")
        raise

if __name__ == "__main__":
    main()
