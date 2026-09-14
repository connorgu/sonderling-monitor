# Sonderling Monitor — Setup Instructions

Monitors Google News and Bing News every 15 minutes for new mentions of
Keith Sonderling and sends email (and optional SMS) alerts.

---

## 1. Prerequisites

- A GitHub account with the GitHub CLI (`gh`) installed and authenticated
- A Gmail account for sending alerts (a dedicated one is recommended)

---

## 2. Gmail App Password

1. Go to your Google Account → Security → 2-Step Verification (must be ON).
2. Under 2-Step Verification, scroll to **App passwords**.
3. Create a new app password:  Select app → Mail, Select device → Other → type "Sonderling Monitor".
4. Copy the 16-character password. You will add it as `GMAIL_APP_PASSWORD`.

---

## 3. Create the GitHub repository

```bash
gh repo create sonderling-monitor --private --source=. --remote=origin --push
```

This creates a private repo, sets origin, and does the initial push.

---

## 4. Add GitHub Actions secrets

Run these commands (fill in your actual values):

```bash
gh secret set GMAIL_USER         --body "you@gmail.com"
gh secret set GMAIL_APP_PASSWORD --body "xxxx xxxx xxxx xxxx"
gh secret set ALERT_EMAIL        --body "alerts@example.com"

# Optional — for SMS alerts via carrier email gateway:
# AT&T:     number@txt.att.net
# T-Mobile: number@tmomail.net
# Verizon:  number@vtext.com
gh secret set ALERT_SMS_GATEWAY  --body "5551234567@tmomail.net"
```

---

## 5. Verify the workflow runs

1. Go to the repository on GitHub.
2. Click **Actions** → **Sonderling Monitor** → **Run workflow**.
3. Wait ~30 seconds, then click the run to see logs.

---

## 6. Adjusting search terms

Edit the `SEARCH_TERMS` list in `sonderling_monitor.py`:

```python
SEARCH_TERMS = [
    "Keith Sonderling",
    "\"Keith Sonderling\"",          # exact phrase
    "Sonderling EEOC",               # topic-specific
]
```

Commit and push — the next workflow run picks up the changes.

---

## 7. Checking run history

- **Actions tab** on GitHub shows every run, its status, and full logs.
- The `seen_items.json` file in the repo records every link already alerted on.
- To reset and re-alert on everything, clear `seen_items.json` back to `[]` and push.
