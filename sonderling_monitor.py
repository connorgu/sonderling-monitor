#!/usr/bin/env python3
"""
Media monitor for Keith Sonderling.
Polls Google News, Bing News, and Reddit RSS feeds for new mentions.
Runs every minute inside a GitHub Actions job (cron fires every 5 min,
script loops internally for faster-than-cron cadence).
Sends an email alert from sonderlingalerts@gmail.com to the ALERT_EMAIL
for every new item found.
"""

import json
import os
import smtplib
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ── Search terms ─────────────────────────────────────────────────────────────
SEARCH_TERMS = [
    '"Keith Sonderling"',
]

# ── Feed templates ────────────────────────────────────────────────────────────
FEED_TEMPLATES = [
    "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en",
    "https://www.bing.com/news/search?q={query}&format=RSS",
    "https://www.reddit.com/search.json?q={query}&sort=new&limit=25",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

SEEN_FILE = Path("seen_items.json")

# ── How long to run inside one Actions job before exiting ─────────────────────
# GitHub Actions jobs time out at 6 h; we keep well under that.
# Set to 0 to run exactly once (useful for manual tests).
LOOP_DURATION_SECONDS = int(os.environ.get("LOOP_DURATION", "240"))  # 4 min
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL", "30"))    # every 30 s


# ── Persistence ───────────────────────────────────────────────────────────────

def load_seen() -> set:
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text()))
        except (json.JSONDecodeError, TypeError):
            pass
    return set()


def save_seen(seen: set) -> None:
    SEEN_FILE.write_text(json.dumps(sorted(seen), indent=2))


# ── Fetching ──────────────────────────────────────────────────────────────────

def fetch_rss(url: str) -> list[dict]:
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
    except Exception as exc:
        print(f"  [warn] RSS fetch failed: {url} — {exc}")
        return []
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        print(f"  [warn] RSS parse failed: {url} — {exc}")
        return []
    items = []
    for item in root.findall(".//item"):
        link = (item.findtext("link") or "").strip()
        if not link:
            continue
        items.append({
            "title":     (item.findtext("title")   or "").strip(),
            "link":      link,
            "published": (item.findtext("pubDate") or "").strip(),
            "source":    url.split("/")[2],
        })
    return items


def fetch_reddit(url: str) -> list[dict]:
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as exc:
        print(f"  [warn] Reddit fetch failed: {url} — {exc}")
        return []
    items = []
    for child in data.get("data", {}).get("children", []):
        d = child.get("data", {})
        link = d.get("url", "").strip()
        if not link:
            continue
        ts = d.get("created_utc", 0)
        pub = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000") if ts else ""
        items.append({
            "title":     d.get("title", "").strip(),
            "link":      link,
            "published": pub,
            "source":    "reddit.com",
        })
    return items


def fetch_all(terms: list[str]) -> list[dict]:
    results = []
    for term in terms:
        encoded = urllib.parse.quote(term)
        for template in FEED_TEMPLATES:
            url = template.format(query=encoded)
            if "reddit.com" in url:
                results.extend(fetch_reddit(url))
            else:
                results.extend(fetch_rss(url))
    return results


# ── Alerting ──────────────────────────────────────────────────────────────────

def _smtp_connection():
    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server.login(os.environ["GMAIL_USER"], os.environ["GMAIL_APP_PASSWORD"])
    return server


def _classify(item: dict) -> str:
    """Guess the type of mention from title/source."""
    title_lower = item["title"].lower()
    source      = item["source"].lower()
    if "reddit" in source:
        return "social media post"
    if any(w in title_lower for w in ("press release", "announces", "statement")):
        return "press release"
    if any(w in title_lower for w in ("interview", "op-ed", "opinion", "column")):
        return "opinion piece"
    return "media hit"


def send_alert(items: list[dict]) -> None:
    gmail_user  = os.environ["GMAIL_USER"]
    alert_email = os.environ["ALERT_EMAIL"]
    now_str     = datetime.now(timezone.utc).strftime("%B %d, %Y at %I:%M %p UTC")

    subject = (
        f"New Mention Alert — Keith Sonderling "
        f"({'1 item' if len(items) == 1 else f'{len(items)} items'})"
    )

    lines = []
    for it in items:
        kind     = _classify(it)
        pub_time = it["published"] or now_str
        lines += [
            f"Mr. Secretary Sonderling,",
            f"",
            f"A new {kind} has been released and your name is included.",
            f"",
            f"Headline  : {it['title']}",
            f"Source    : {it['source']}",
            f"Posted at : {pub_time}",
            f"Link      : {it['link']}",
            f"",
            f"{'─' * 60}",
            f"",
        ]

    lines.append(f"— Sonderling Monitor  |  Alert generated {now_str}")
    body = "\n".join(lines)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Sonderling Monitor <{gmail_user}>"
    msg["To"]      = alert_email
    msg.attach(MIMEText(body, "plain"))

    with _smtp_connection() as server:
        server.sendmail(gmail_user, [alert_email], msg.as_string())

    print(f"  [alert] Email sent to {alert_email} — {len(items)} item(s)")


# ── Main loop ─────────────────────────────────────────────────────────────────

def poll_once(seen: set) -> tuple[set, int]:
    """Fetch all feeds, alert on new items, return updated seen set + count."""
    all_items = fetch_all(SEARCH_TERMS)

    new_items: list[dict] = []
    seen_this_round: set  = set()
    for it in all_items:
        if it["link"] not in seen and it["link"] not in seen_this_round:
            new_items.append(it)
            seen_this_round.add(it["link"])

    if new_items:
        send_alert(new_items)
        for it in new_items:
            seen.add(it["link"])
        save_seen(seen)

    return seen, len(new_items)


def main() -> None:
    seen = load_seen()
    print(f"[start] {len(seen)} previously seen items loaded.")
    print(f"[start] Polling every {POLL_INTERVAL_SECONDS}s for {LOOP_DURATION_SECONDS}s total.")

    deadline = time.monotonic() + LOOP_DURATION_SECONDS
    total_new = 0

    while True:
        loop_start = time.monotonic()
        print(f"\n[poll] {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")
        seen, n = poll_once(seen)
        total_new += n
        print(f"[poll] {n} new item(s) this round (total this run: {total_new})")

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        sleep_for = min(POLL_INTERVAL_SECONDS, remaining)
        elapsed   = time.monotonic() - loop_start
        sleep_for = max(0, sleep_for - elapsed)
        if sleep_for > 0:
            print(f"[poll] Sleeping {sleep_for:.0f}s …")
            time.sleep(sleep_for)

    print(f"\n[done] Run complete. {total_new} total new mention(s) alerted.")


if __name__ == "__main__":
    main()
