# DS/ML/Analytics internship hourly email alert

Runs every hour via GitHub Actions. Fetches from [intern-list.com](https://www.intern-list.com/da-intern-list) (Data Analyst list = DS/analytics/ML) and [jobright.ai](https://jobright.ai/jobs/data-scientist-intern-jobs-in-united-states). Emails only **new** listings from the last hour; no repeats (dedup via cached `seen_ids.txt`). Sorted latest first.

**Setup:** Add repo secrets: `EMAIL_TO`, `EMAIL_FROM` (optional, defaults to EMAIL_TO), `EMAIL_APP_PASSWORD` (Gmail app password). Then push; workflow runs on cron `0 * * * *` or trigger manually.

**Local run:** `EMAIL_TO=you@example.com EMAIL_APP_PASSWORD=xxx python3 alert.py` (prints body if secrets missing).
