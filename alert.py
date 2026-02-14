#!/usr/bin/env python3
"""Hourly DS/Analytics/ML internship alerts from intern-list + jobright + Airtable table. Dedup via seen_ids file."""
import os, re, json, smtplib, ssl
from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo
    _EST = ZoneInfo("America/New_York")
except ImportError:
    _EST = timezone(timedelta(hours=-5))  # EST fallback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.request import Request, urlopen, HTTPSHandler, build_opener, HTTPCookieProcessor
from http.cookiejar import CookieJar

# Use certifi on macOS so HTTPS works locally (GitHub runner has certs)
def _ssl_context():
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()

_opener = build_opener(HTTPSHandler(context=_ssl_context()), HTTPCookieProcessor(CookieJar()))

SEEN_FILE = os.environ.get("SEEN_FILE", "seen_ids.txt")
# Webflow CMS page (scrapeable). ?k=da = Data Analysis on main page (hourly list).
INTERN_LIST_URL = "https://www.intern-list.com/da-intern-list"
INTERN_LIST_MAIN_URL = "https://www.intern-list.com/?k=da"
# Airtable embed (Data Analysis view) - we fetch embed page then call its readSharedViewData API.
AIRTABLE_EMBED_URL = "https://airtable.com/embed/app742LMLO7tQP9dO/shrxLJiBa4dfQZwx8?viewControls=on"
JOBRIGHT_URL = "https://jobright.ai/jobs/data-scientist-intern-jobs-in-united-states"
# Jobright minisites (__NEXT_DATA__.initialJobs, postedDate in ms): Data Analysis, AI/ML, Business Analyst
JOBRIGHT_DA_URL = "https://jobright.ai/minisites-jobs/intern/us/data_analysis"
JOBRIGHT_AIML_URL = "https://jobright.ai/minisites-jobs/intern/us/aiml"
JOBRIGHT_BA_URL = "https://jobright.ai/minisites-jobs/intern/us/business_analyst"
JOBRIGHT_MINISITES = (JOBRIGHT_DA_URL, JOBRIGHT_AIML_URL, JOBRIGHT_BA_URL)
WINDOW_MINS = 120  # include jobs posted in last 2 hours
def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_seen(seen):
    with open(SEEN_FILE, "w") as f:
        f.write("\n".join(sorted(seen)))

def fetch(url, headers=None, timeout=15):
    req = Request(url, headers=headers or {"User-Agent": "Mozilla/5.0"})
    return _opener.open(req, timeout=timeout).read().decode("utf-8", errors="replace")

def fetch_with_retry(url, timeout=30, retries=2):
    for attempt in range(retries + 1):
        try:
            return fetch(url, timeout=timeout)
        except Exception as e:
            if attempt == retries:
                raise
            print(f"  fetch retry {attempt + 1}/{retries} for {url[:50]}...: {e}")

def fetch_airtable_table():
    """Try Airtable readSharedViewData API (often 401); fallback to Playwright below."""
    try:
        html = fetch(AIRTABLE_EMBED_URL)
        m = re.search(r'fetch\("([^"]*readSharedViewData[^"]+)"', html)
        if not m:
            return None
        path = m.group(1).replace("\\u002F", "/").replace("\\u0026", "&")
        if not path.startswith("/"):
            return None
        api_url = "https://airtable.com" + path
        r = _opener.open(
            Request(
                api_url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": AIRTABLE_EMBED_URL,
                    "x-airtable-application-id": "app742LMLO7tQP9dO",
                    "Accept": "application/json",
                },
            ),
            timeout=20,
        )
        return json.loads(r.read().decode())
    except Exception:
        return None

# Relative time: support many formats (hours/minutes ago, with or without 's', hr/hrs/h, min/mins/m).
# Search anywhere in string (not just match from start). Return (minutes, matched_str) or (None, None).
_RELATIVE_TIME_PATTERN = re.compile(
    r"(\d+)\s*(?:hours?|hrs?|h|minutes?|mins?|m)\s+ago",
    re.I
)
def parse_relative_time(text):
    if not text:
        return None, None
    m = _RELATIVE_TIME_PATTERN.search(text.strip())
    if not m:
        return None, None
    n, raw = int(m.group(1)), m.group(0).strip()
    low = raw.lower()
    # minutes: "30 mins ago", "30m ago", "1 minute ago". Hours: "2h ago", "2 hours ago", "2hrs ago".
    if "min" in low:
        mins = n
    else:
        mins = n * 60
    return mins, raw

def within_last_2hr(date_str):
    mins, _ = parse_relative_time(date_str)
    return mins is not None and mins <= WINDOW_MINS

def job_in_window(j):
    """True if job is within last WINDOW_MINS: by relative posted_str ('X hours ago') or by absolute posted datetime.
    For date-only postings (e.g. intern-list 'February 13, 2026'), treat today and yesterday as in-window."""
    if within_last_2hr(j.get("posted_str") or ""):
        return True
    pt = j.get("posted")
    if pt is not None:
        if pt.tzinfo is None:
            pt = pt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta_sec = (now - pt).total_seconds()
        if 0 <= delta_sec <= WINDOW_MINS * 60:
            return True
        # Date-only (no time): include if posted date is today or yesterday
        if delta_sec > 0 and pt.hour == 0 and pt.minute == 0:
            posted_date = pt.date()
            if posted_date >= (now - timedelta(days=1)).date():
                return True
    return False

def format_est(ms):
    """Format Unix timestamp (ms) as Eastern time (EST/EDT)."""
    if ms is None or ms <= 0:
        return None
    try:
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).astimezone(_EST)
        z = dt.strftime("%Z")
        if not z or z.startswith("-") or z.startswith("+"):
            z = "EDT" if (dt.dst() and dt.dst().total_seconds()) else "EST"
        return dt.strftime("%b %d, %Y %I:%M %p ") + z
    except Exception:
        return None

# Guardrails: min pay $25+/hr, USA only
def meets_min_pay(salary_str, min_hr=25):
    """True if salary_str indicates >= min_hr $/hr. Parses '$25-$30/hr', '$66362-$83000/yr', 'N/A'."""
    if not salary_str or not isinstance(salary_str, str):
        return False
    s = salary_str.strip().upper()
    if s in ("N/A", "NA", ""):
        return False
    m = re.match(r"\$?\s*(\d+)(?:\s*-\s*\$?\s*(\d+))?\s*/\s*(HR|YR)", s)
    if not m:
        return False
    low = int(m.group(1))
    unit = m.group(3)
    if unit == "YR":
        low = low / 2080
    return low >= min_hr

def is_usa_location(location_str):
    """True if location is USA or US-based. Exclude Canada, UK, etc."""
    if not location_str or not isinstance(location_str, str):
        return True
    loc = location_str.strip()
    if not loc:
        return True
    loc_lower = loc.lower()
    if any(x in loc_lower for x in ("canada", "ontario", "quebec", "uk", "united kingdom", "london", "europe", "india", "australia", "toronto", "vancouver", "calgary", "brisbane")):
        return False
    if any(x in loc_lower for x in ("united states", "usa", " u.s.", " us,", "remote", "multi locations")):
        return True
    if re.search(r",\s*[A-Z]{2}\s*(?:,|$)", loc):
        return True
    us_states = r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\b"
    if re.search(us_states, loc, re.I):
        return True
    return True

def _airtable_date_recent(date_str):
    """True if date_str is YYYY-MM-DD and is today or yesterday UTC (Airtable shows raw date; we include recent rows)."""
    if not date_str or not date_str.strip():
        return False, None
    s = date_str.strip()[:10]
    try:
        d = datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return False, None
    today = datetime.now(timezone.utc).date()
    if d == today or d == today - timedelta(days=1):
        return True, s
    return False, None

def scrape_airtable_playwright():
    """Load Airtable embed; include rows with 'X hours ago' ≤2hr, or date column = today/yesterday (all listings are in Airtable)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return []
    rows = []
    with sync_playwright() as p:
        try:
            browser = p.firefox.launch(headless=True)
        except Exception:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"])
        try:
            page = browser.new_page()
            page.goto(AIRTABLE_EMBED_URL, wait_until="load", timeout=45000)
            page.wait_for_selector(".dataRow", timeout=20000)
            page.wait_for_timeout(6000)
            # Airtable: .dataLeftPane + .dataRightPane, pair .dataRow by index. Get cells + full row text (catches "X hours ago" in any format).
            raw = page.evaluate("""() => {
                const leftRows = document.querySelectorAll('.dataLeftPane .dataRow[data-rowid]');
                const rightRows = document.querySelectorAll('.dataRightPane .dataRow[data-rowid]');
                const result = [];
                const n = Math.min(leftRows.length, rightRows.length);
                for (let i = 0; i < n; i++) {
                    const leftCells = Array.from(leftRows[i].querySelectorAll('.cell')).map(c => (c.innerText || c.textContent || '').trim()).filter(Boolean);
                    const rightCells = Array.from(rightRows[i].querySelectorAll('.cell')).map(c => (c.innerText || c.textContent || '').trim()).filter(Boolean);
                    const rowText = ((leftRows[i].innerText || '') + ' ' + (rightRows[i].innerText || '')).trim();
                    result.push([...leftCells, ...rightCells, rowText]);
                }
                return result;
            }""")
            if os.environ.get("DEBUG"):
                for i, cell_list in enumerate((raw or [])[:3]):
                    print(f"DEBUG Airtable row {i}: {cell_list[:8]}")  # first 8 cells
            for cell_list in (raw or []):
                if len(cell_list) < 2:
                    continue
                title = (cell_list[0] or "")[:200]
                company = (cell_list[5] or "")[:100] if len(cell_list) > 5 else ""
                if not title:
                    continue
                # Prefer relative time "X hours ago" ≤2hr; else accept date column = today/yesterday (Airtable stores date, we include recent)
                date_str = ""
                full_row = " ".join(str(c) for c in cell_list)
                for chunk in cell_list + [full_row]:
                    mins, matched = parse_relative_time(str(chunk))
                    if mins is not None and mins <= WINDOW_MINS and matched:
                        date_str = matched
                        break
                if not date_str:
                    for c in cell_list:
                        ok, day = _airtable_date_recent(str(c))
                        if ok and day:
                            date_str = day
                            break
                if not date_str:
                    continue
                rows.append({
                    "id": f"at_{abs(hash((title, company, date_str))) % 10**10}",
                    "title": title, "company": company,
                    "url": AIRTABLE_EMBED_URL, "posted": None, "posted_str": date_str
                })
        finally:
            browser.close()
    return rows

def parse_airtable(data):
    """Parse readSharedViewData response into jobs. Expect rows with cell values; filter ≤2hr."""
    jobs = []
    if not data or "data" not in data:
        return jobs
    # Response can have data.visibleRecordIds + data.rowsById or similar
    rows = data.get("data", {}).get("rowsById") or data.get("data", {}).get("rows") or {}
    if isinstance(rows, dict):
        rows = list(rows.values()) if rows else []
    # Also try table/cell structure
    table = data.get("data", {}).get("table") or data.get("data", {})
    if not rows and "rows" in table:
        rows = table["rows"]
    if not rows and "cellValuesByColumnId" in str(data):
        # Nested: might be data.rows with list of row objects
        d = data.get("data", {})
        for key in ("rows", "visibleRows", "recordIds"):
            if key in d and isinstance(d[key], list):
                rows = d[key]
                break
    # Find column mapping from metadata if present
    meta = data.get("data", {}).get("table", {}).get("columns") or data.get("data", {}).get("view", {}).get("fieldOrder") or []
    # Heuristic: row dict with displayValue or value; look for "Position Title", "Date", "Company"
    def row_to_job(row):
        if isinstance(row, dict):
            # Cell values keyed by field name or id
            title = (row.get("Position Title") or row.get("positionTitle") or row.get("title") or
                     (row.get("cells") or {}).get("Position Title") or "")
            date_str = (row.get("Date") or row.get("date") or (row.get("cells") or {}).get("Date") or "")
            company = (row.get("Company") or row.get("company") or (row.get("cells") or {}).get("Company") or "")
            link = (row.get("Apply") or row.get("Apply URL") or (row.get("cells") or {}).get("Apply") or "")
            if isinstance(title, dict):
                title = title.get("displayValue", title.get("value", ""))
            if isinstance(date_str, dict):
                date_str = date_str.get("displayValue", date_str.get("value", ""))
            if isinstance(company, dict):
                company = company.get("displayValue", company.get("value", ""))
            title = str(title)[:200] if title else ""
            date_str = str(date_str) if date_str else ""
            company = str(company)[:100] if company else ""
            mins, matched = parse_relative_time(date_str)
            if mins is None or mins > WINDOW_MINS:
                return None
            return {"id": f"at_{abs(hash((title, company, date_str))) % 10**10}", "title": title, "company": company,
                    "url": link if isinstance(link, str) and link.startswith("http") else AIRTABLE_EMBED_URL,
                    "posted": None, "posted_str": matched or date_str or "—"}
        return None
    seen = set()
    for row in (rows if isinstance(rows, list) else list(rows.values())):
        j = row_to_job(row) if isinstance(row, dict) else row_to_job({"cells": row} if isinstance(row, (list, dict)) else {})
        if j and j["id"] not in seen:
            seen.add(j["id"])
            jobs.append(j)
    return jobs

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

def parse_jobright_next_data(html):
    """Extract jobs from Jobright minisites __NEXT_DATA__ (initialJobs with postedDate in ms). Keep only ≤2hr."""
    jobs = []
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not m:
        return jobs
    try:
        data = json.loads(m.group(1).strip())
        initial = (data.get("props") or {}).get("pageProps") or {}
        job_list = initial.get("initialJobs") or []
    except (json.JSONDecodeError, TypeError):
        return jobs
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    window_ms = WINDOW_MINS * 60 * 1000
    for j in job_list:
        if not isinstance(j, dict):
            continue
        jid = j.get("id") or ""
        posted_ms = j.get("postedDate")
        if not jid or posted_ms is None:
            continue
        try:
            posted_ms = int(posted_ms)
        except (TypeError, ValueError):
            continue
        diff_ms = now_ms - posted_ms
        if diff_ms < 0 or diff_ms > window_ms:
            continue
        mins_ago = diff_ms // (60 * 1000)
        if mins_ago < 60:
            posted_str = f"{mins_ago} min ago" if mins_ago == 1 else f"{mins_ago} mins ago"
        else:
            hrs = mins_ago // 60
            posted_str = f"{hrs} hour ago" if hrs == 1 else f"{hrs} hours ago"
        if not meets_min_pay(j.get("salary") or ""):
            continue
        if not is_usa_location(j.get("location") or ""):
            continue
        title = (j.get("title") or "")[:200]
        company = (j.get("company") or "")[:100]
        url = (j.get("applyUrl") or "").strip()
        if not url or not url.startswith("http"):
            url = f"https://jobright.ai/jobs/info/{jid}"
        jobs.append({
            "id": f"jr_{jid}",
            "title": title or "Data Analysis Intern",
            "company": company,
            "url": url.split("?")[0] if "?" in url else url,
            "posted": None,
            "posted_str": posted_str,
            "posted_ms": posted_ms,
            "posted_est": format_est(posted_ms),
        })
    return jobs

def parse_jobright(html):
    jobs, seen = [], set()
    for m in re.finditer(r"https://jobright\.ai/jobs/info/([a-f0-9]+)", html):
        jid = m.group(1)
        if jid in seen:
            continue
        # Find "X hours ago" / "Xh ago" / "30 mins ago" near this link (search preceding + following text)
        start = max(0, m.start() - 120)
        end = min(len(html), m.end() + 120)
        chunk = html[start:end]
        mins, time_str = parse_relative_time(chunk)
        if mins is not None and mins <= WINDOW_MINS and time_str:
            title_m = re.search(r"\[([^\]]+)\]", chunk)
            title = title_m.group(1).strip() if title_m else "Data Scientist Intern"
            seen.add(jid)
            jobs.append({
                "id": f"jr_{jid}", "title": title[:200], "company": "",
                "url": f"https://jobright.ai/jobs/info/{jid}", "posted": None, "posted_str": time_str
            })
    return jobs

def scrape_jobright_playwright():
    """Use headless browser to load Jobright and extract job cards (≤2hr)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return []
    jobs = []
    with sync_playwright() as p:
        try:
            browser = p.firefox.launch(headless=True)
        except Exception:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"])
        try:
            page = browser.new_page()
            page.goto(JOBRIGHT_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(6000)
            # Job cards: links to /jobs/info/ID; link text often "X hours ago" + title. Get parent card text too.
            raw = page.evaluate("""() => {
                const links = document.querySelectorAll('a[href*="/jobs/info/"]');
                return Array.from(links).map(a => {
                    const href = a.getAttribute('href') || '';
                    const m = href.match(/\\/jobs\\/info\\/([a-f0-9]+)/);
                    const id = m ? m[1] : '';
                    const text = (a.innerText || a.textContent || '').trim();
                    const parent = a.closest('[class*="card"], [class*="job"], [class*="item"], tr, li') || a.parentElement;
                    const parentText = parent ? (parent.innerText || parent.textContent || '').trim().slice(0, 500) : '';
                    return { id, href: href.split('?')[0], text, parentText };
                }).filter(x => x.id);
            }""")
            seen = set()
            for item in (raw or []):
                jid = item.get("id")
                href = item.get("href", "")
                text = (item.get("text") or "")
                parent_text = (item.get("parentText") or "")
                if not jid or jid in seen:
                    continue
                # Parse relative time from link text or parent (formats: "2 hours ago", "2h ago", "30 mins ago")
                mins, time_str = parse_relative_time(text)
                if mins is None:
                    mins, time_str = parse_relative_time(parent_text)
                if mins is None or mins > WINDOW_MINS or not time_str:
                    continue
                # Prefer title as line that looks like a role (often after "X hours ago" or first line of link text)
                title = text.replace(time_str, "").strip() if time_str in text else text
                if "\n" in title:
                    title = title.split("\n")[0].strip()
                if not title or len(title) < 3:
                    title = "Data Scientist Intern"
                seen.add(jid)
                jobs.append({
                    "id": f"jr_{jid}", "title": title[:200], "company": "",
                    "url": href if href.startswith("http") else f"https://jobright.ai/jobs/info/{jid}",
                    "posted": None, "posted_str": time_str
                })
        finally:
            browser.close()
    return jobs

def scrape_intern_list_playwright():
    """Load intern-list with Playwright; look for 'X hours ago' in page or iframe (Hourly Updated table)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return []
    jobs = []
    with sync_playwright() as p:
        try:
            browser = p.firefox.launch(headless=True)
        except Exception:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"])
        try:
            page = browser.new_page()
            # Load main Data Analysis page (?k=da) where hourly list with "2 hours ago" may appear
            page.goto(INTERN_LIST_MAIN_URL, wait_until="load", timeout=30000)
            page.wait_for_timeout(10000)
            # Main frame only (iframes are often cross-origin and inaccessible). Find rows with "X hours ago".
            raw = page.evaluate("""() => {
                const out = [];
                const rows = document.querySelectorAll('tr, [class*="row"], [class*="Row"], .dataRow, [role="row"]');
                for (const row of rows) {
                    const text = (row.innerText || row.textContent || '').replace(/\\s+/g, ' ');
                    if (!/\\d+\\s*(?:hours?|hrs?|h|minutes?|mins?|m)\\s+ago/i.test(text)) continue;
                    const link = row.querySelector('a[href*="/da-intern-list/"], a[href*="airtable"], a[href*="apply"]');
                    const href = link ? (link.href || link.getAttribute('href') || '') : '';
                    const cells = Array.from(row.querySelectorAll('td, th, [class*="cell"], .cell')).map(c => (c.innerText || c.textContent || '').trim()).filter(Boolean);
                    out.push({ cells, text: text.slice(0, 600), href });
                }
                return out;
            }""")
            for item in (raw or []):
                cells = item.get("cells") or []
                text = item.get("text") or ""
                href = item.get("href") or ""
                mins, date_str = parse_relative_time(text)
                if mins is None or mins > WINDOW_MINS or not date_str:
                    continue
                title = (cells[0] or "")[:200] if cells else ""
                if not title and "Apply" in text:
                    parts = text.split("Apply")[0].strip().split()
                    for i, p in enumerate(parts):
                        if parse_relative_time(p)[0] is not None:
                            title = " ".join(parts[:i]).strip() or "Intern"
                            break
                if not title:
                    title = text.split(date_str)[0].strip()[:200] or "Intern"
                company = (cells[5] or cells[-1] or "")[:100] if len(cells) > 5 else ""
                url = href if href.startswith("http") else (INTERN_LIST_URL if not href else (INTERN_LIST_URL.rstrip("/") + "/" + href.lstrip("/")))
                job_id = f"il_pw_{abs(hash((title, company, date_str))) % 10**10}"
                jobs.append({"id": job_id, "title": title, "company": company, "url": url, "posted": None, "posted_str": date_str})
        finally:
            browser.close()
    return jobs

def main():
    seen = load_seen()
    all_jobs = []
    today_utc = datetime.now(timezone.utc).date()
    now_est = datetime.now(timezone.utc).astimezone(_EST).strftime("%b %d, %Y %I:%M %p %Z")
    print(f"Current time EST: {now_est}")

    try:
        html = fetch(INTERN_LIST_URL)
        parsed = parse_intern_list(html, today_utc)
        for j in parsed:
            if not job_in_window(j):
                continue
            new = j["id"] not in seen
            seen.add(j["id"])
            if new:
                all_jobs.append((j, "intern-list"))
        in_window = sum(1 for j in parsed if job_in_window(j))
        if in_window == 0:
            il_pw = scrape_intern_list_playwright()
            for j in il_pw:
                new = j["id"] not in seen
                seen.add(j["id"])
                if new:
                    all_jobs.append((j, "intern-list"))
            if il_pw:
                print(f"intern-list: {len(parsed)} parsed (static), {len(il_pw)} ≤2hr from Playwright, {len([x for x in all_jobs if x[1]=='intern-list'])} new")
            else:
                print(f"intern-list: {len(parsed)} parsed, {in_window} ≤2hr, {len([x for x in all_jobs if x[1]=='intern-list'])} new")
        else:
            print(f"intern-list: {len(parsed)} parsed, {in_window} ≤2hr, {len([x for x in all_jobs if x[1]=='intern-list'])} new")
    except Exception as e:
        print(f"intern-list fetch error: {e}")

    try:
        # Fetch all 3 minisites (Data Analysis, AI/ML, Business Analyst), combine and dedupe by id
        jr_parsed = []
        seen_jr = set()
        for url in JOBRIGHT_MINISITES:
            html = fetch_with_retry(url, timeout=30)
            for j in parse_jobright_next_data(html):
                if j["id"] not in seen_jr:
                    seen_jr.add(j["id"])
                    jr_parsed.append(j)
        if not jr_parsed:
            html = fetch_with_retry(JOBRIGHT_URL, timeout=30)
            jr_parsed = parse_jobright(html)
        if not jr_parsed:
            jr_parsed = scrape_jobright_playwright()
        for j in jr_parsed:
            new = j["id"] not in seen
            seen.add(j["id"])
            if new:
                all_jobs.append((j, "jobright"))
        print(f"jobright: {len(jr_parsed)} parsed (≤2hr), {len([x for x in all_jobs if x[1]=='jobright'])} new")
    except Exception as e:
        print(f"jobright fetch error: {e}")

    # Airtable table (embed): API often 401; use Playwright if available
    try:
        data = fetch_airtable_table()
        if data:
            at_jobs = parse_airtable(data)
            for j in at_jobs:
                new = j["id"] not in seen
                seen.add(j["id"])
                if new:
                    all_jobs.append((j, "airtable"))
            print(f"airtable (API): {len(at_jobs)} parsed")
        else:
            pw_jobs = scrape_airtable_playwright()
            for j in pw_jobs:
                new = j["id"] not in seen
                seen.add(j["id"])
                if new:
                    all_jobs.append((j, "airtable"))
            print(f"airtable (Playwright): {len(pw_jobs)} parsed (≤2hr)")
    except Exception as e:
        print(f"airtable error: {e}")

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
        disp = j.get("posted_est") or j["posted_str"]
        print(f"  [{src}] {t} | {disp}")
    if len(to_send) > 20:
        print(f"  ... and {len(to_send) - 20} more (see email).")

    if not to_send:
        return

    body = f"DS/Analytics/ML internships (new in last 2hr, latest first)\n(Current time: {now_est})\n\n"
    for j, src in to_send:
        disp = j.get("posted_est") or j["posted_str"]
        body += f"- {j['title']} | {j['company']}\n  Posted: {disp}\n  {j['url']}\n\n"

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
