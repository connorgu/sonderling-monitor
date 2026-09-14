#!/usr/bin/env python3
"""
Media monitor for Keith Sonderling.
Checks Google News and Bing News RSS feeds for new mentions and sends
email (and optional SMS-via-email) alerts for anything not yet seen.
"""

import json
import os
import smtplib
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

SEARCH_TERMS = [
    "Keith Sonderling",
]

SEEN_FILE = Path("seen_items.json")

GOOGLE_RSS = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
BING_RSS   = "https://www.bing.com/news/search?q={query}&format=RSS"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SonderlingMonitor/1.0)"}


def load_seen() -> set:
    if SEEN_FILE.exists():
        try:
            data = json.loads(SEEN_FILE.read_text())
            return set(data)
        except (json.JSONDecodeError, TypeError):
            pass
    return set()


def save_seen(seen: set) -> None:
    SEEN_FILE.write_text(json.dumps(sorted(seen), indent=2))


def fetch_feed(url: str) -> list[dict]:
    """Fetch an RSS feed and return a list of {title, link, published} dicts."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
    except Exception as exc:
        print(f"[WARN] Could not fetch {url}: {exc}")
        return []

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        print(f"[WARN] Could not parse feed {url}: {exc}")
        return []

    items = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link  = (item.findtext("link")  or "").strip()
        pub   = (item.findtext("pubDate") or "").strip()
        if link:
            items.append({"title": title, "link": link, "published": pub})
    return items


def fetch_all_items() -> list[dict]:
    items = []
    for term in SEARCH_TERMS:
        encoded = urllib.parse.quote(term)
        for template in (GOOGLE_RSS, BING_RSS):
            url = template.format(query=encoded)
            items.extend(fetch_feed(url))
    return items


def send_email(subject: str, body: str) -> None:
    gmail_user     = os.environ["GMAIL_USER"]
    gmail_password = os.environ["GMAIL_APP_PASSWORD"]
    alert_email    = os.environ["ALERT_EMAIL"]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = gmail_user
    msg["To"]      = alert_email
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, [alert_email], msg.as_string())
    print(f"[INFO] Email sent to {alert_email}")


def send_sms(subject: str, body: str) -> None:
    sms_gateway = os.environ.get("ALERT_SMS_GATEWAY", "").strip()
    if not sms_gateway:
        return

    gmail_user     = os.environ["GMAIL_USER"]
    gmail_password = os.environ["GMAIL_APP_PASSWORD"]

    short_body = f"{subject}\n{body}"[:160]
    msg = MIMEText(short_body)
    msg["Subject"] = subject
    msg["From"]    = gmail_user
    msg["To"]      = sms_gateway

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, [sms_gateway], msg.as_string())
    print(f"[INFO] SMS sent to {sms_gateway}")


def main() -> None:
    seen = load_seen()
    print(f"[INFO] Loaded {len(seen)} previously seen items.")

    all_items = fetch_all_items()
    print(f"[INFO] Fetched {len(all_items)} items from feeds.")

    new_items = [it for it in all_items if it["link"] not in seen]

    # Deduplicate by link within this batch
    seen_links_this_run: set = set()
    deduped = []
    for it in new_items:
        if it["link"] not in seen_links_this_run:
            deduped.append(it)
            seen_links_this_run.add(it["link"])
    new_items = deduped

    if not new_items:
        print("[INFO] No new items found.")
        return

    print(f"[INFO] {len(new_items)} new item(s) found — sending alert.")

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    subject = f"[Sonderling Alert] {len(new_items)} new mention(s) — {now_str}"

    lines = [f"Keith Sonderling Media Alert — {now_str}", ""]
    for it in new_items:
        lines.append(f"• {it['title']}")
        lines.append(f"  {it['link']}")
        if it["published"]:
            lines.append(f"  Published: {it['published']}")
        lines.append("")
    body = "\n".join(lines)

    send_email(subject, body)
    send_sms(subject, body)

    for it in new_items:
        seen.add(it["link"])
    save_seen(seen)
    print(f"[INFO] Updated seen_items.json with {len(new_items)} new link(s).")


if __name__ == "__main__":
    main()
