#!/usr/bin/env python3
"""
Keith Sonderling Media Monitor — 24/7 real-time alert system
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RECENCY GATE (strict): confirmed within MAX_AGE_MINUTES or DROPPED
CONCURRENCY: all 40+ sources fetched in parallel every poll cycle
SOURCES: Google News (×4 keywords), Bing News (×4), 30+ direct outlets,
         Reddit (×4), Google Alerts RSS (×4)
KEYWORDS: "Keith Sonderling" · "secretary of labor" · "labor secretary" · "Sonderling"
LATENCY:  Cron every 5 min → polls every 20 s for 4.5 min → ≤ 50 s worst case
"""
import json, os, smtplib, time, urllib.parse, urllib.request, xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime
from pathlib import Path

MAX_AGE_MINUTES = 30

SEARCH_TERMS = [
    '"Keith Sonderling"',
    '"secretary of labor" Sonderling',
    '"labor secretary" Sonderling',
    '"Sonderling"',
]

KEYWORDS = ["sonderling", "secretary of labor", "labor secretary"]

RSS_FEEDS = [
    ("https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en", "Google News"),
    ("https://www.bing.com/news/search?q={query}&format=RSS", "Bing News"),
]

DIRECT_FEEDS = [
    ("https://feeds.foxnews.com/foxnews/politics",               "Fox News"),
    ("https://feeds.foxnews.com/foxnews/latest",                 "Fox News (all)"),
    ("https://feeds.foxbusiness.com/foxbusiness/latest",         "Fox Business"),
    ("https://rss.cnn.com/rss/edition.rss",                      "CNN"),
    ("https://feeds.nbcnews.com/nbcnews/public/politics",        "NBC News"),
    ("https://abcnews.go.com/abcnews/politicsheadlines",         "ABC News"),
    ("https://www.cbsnews.com/latest/rss/politics",              "CBS News"),
    ("https://feeds.washingtonpost.com/rss/politics",            "Washington Post"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml","NY Times"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml","NY Times Business"),
    ("https://feeds.reuters.com/reuters/politicsNews",           "Reuters"),
    ("https://feeds.reuters.com/reuters/topNews",                "Reuters Top"),
    ("https://feeds.a.dj.com/rss/RSSWorldNews.xml",              "Wall Street Journal"),
    ("https://www.politico.com/rss/politicopicks.xml",           "Politico"),
    ("https://thehill.com/feed/",                                "The Hill"),
    ("https://www.npr.org/rss/rss.php?id=1014",                  "NPR Politics"),
    ("https://www.axios.com/feeds/feed.rss",                     "Axios"),
    ("https://www.breitbart.com/feed/",                          "Breitbart"),
    ("https://justthenews.com/feed",                             "Just The News"),
    ("https://www.nationalreview.com/feed/",                     "National Review"),
    ("https://spectator.org/feed/",                              "The Spectator"),
    ("https://www.realclearpolitics.com/index.xml",              "RealClearPolitics"),
    ("https://nypost.com/feed/",                                 "NY Post"),
    ("https://apnews.com/rss",                                   "AP News"),
    ("https://www.dol.gov/rss/releases.xml",                     "Dept of Labor"),
    ("https://www.whitehouse.gov/feed/",                         "White House"),
    ("https://www.congress.gov/rss/congressional-record.xml",    "Congress"),
    ("https://www.bls.gov/feed/bls_latest.rss",                  "BLS"),
    ("https://news.yahoo.com/rss/",                              "Yahoo News"),
    ("https://finance.yahoo.com/news/rssindex",                  "Yahoo Finance"),
    ("https://news.yahoo.com/rss/politics",                      "Yahoo Politics"),
    ("https://www.c-span.org/feeds/clips.rss",                   "C-SPAN"),
]

_GA_RAW = os.environ.get("GOOGLE_ALERTS_RSS", "").strip()
GOOGLE_ALERTS_FEEDS: list[str] = [u.strip() for u in _GA_RAW.split(",") if u.strip()]

REDDIT_URL = "https://www.reddit.com/search.json?q={query}&sort=new&limit=25&type=link"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SEEN_FILE     = Path("seen_items.json")
LOOP_DURATION = int(os.environ.get("LOOP_DURATION", "270"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "20"))
MAX_WORKERS   = int(os.environ.get("MAX_WORKERS",   "50"))

STRIP_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "ref", "referer", "referrer", "source", "via", "ceid", "mkt",
    "c", "aid", "tid", "ref_src", "ref_url",
}


# ── Recency gate ──────────────────────────────────────────────────────────────

def _is_recent(pub_str: str, label: str = "", title: str = "") -> bool:
    """STRICT: True only when publish date is confirmed within MAX_AGE_MINUTES."""
    if not pub_str:
        print(f"    [drop-nodate] {label}: {title[:60]}")
        return False
    try:
        pub_dt = parsedate_to_datetime(pub_str)
        if pub_dt.tzinfo is None:
            pub_dt = pub_dt.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - pub_dt
        if age < timedelta(minutes=-10):
            print(f"    [drop-future] {label} ({pub_str[:25]}): {title[:50]}")
            return False
        if age > timedelta(minutes=MAX_AGE_MINUTES):
            mins = int(age.total_seconds() // 60)
            print(f"    [drop-old {mins}m] {label} ({pub_str[:25]}): {title[:50]}")
            return False
        return True
    except Exception as exc:
        print(f"    [drop-baddate] {label} '{pub_str[:25]}' — {exc}: {title[:50]}")
        return False


# ── Persistence ───────────────────────────────────────────────────────────────

def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text()))
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    return set()


def save_seen(seen: set[str]) -> None:
    SEEN_FILE.write_text(json.dumps(sorted(seen), indent=2))


# ── Network ───────────────────────────────────────────────────────────────────

def _get(url: str) -> bytes | None:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.read()
    except Exception as exc:
        print(f"  [warn] {url[:80]}  →  {exc}")
        return None


def _canonical(url: str) -> str:
    """Unwrap redirect URLs; strip tracking params; normalise for dedup."""
    try:
        # Step 1: unwrap Google News redirect
        if "news.google.com" in url and "url=" in url:
            inner = urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get("url", [None])[0]
            if inner:
                url = inner
        # Step 2: unwrap Bing News redirect
        if "bing.com/news/apiclick" in url and "url=" in url:
            inner = urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get("url", [None])[0]
            if inner:
                url = urllib.parse.unquote(inner)
        # Step 3: strip tracking params; normalise
        parsed = urllib.parse.urlparse(url)
        qs = {k: v for k, v in urllib.parse.parse_qs(parsed.query).items()
              if k.lower() not in STRIP_PARAMS}
        return urllib.parse.urlunparse((
            parsed.scheme.lower(),
            parsed.netloc.lower().lstrip("www."),
            parsed.path.rstrip("/"),
            parsed.params,
            urllib.parse.urlencode(qs, doseq=True),
            "",
        ))
    except Exception:
        return url


# ── Per-feed fetch workers (called concurrently) ──────────────────────────────

def _fetch_rss(url: str, label: str, keyword_filter: bool) -> list[dict]:
    raw = _get(url)
    if not raw:
        return []
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        print(f"  [warn] parse error ({label}): {exc}")
        return []
    items = []
    for item in root.findall(".//item"):
        link = (item.findtext("link") or "").strip()
        if not link:
            continue
        title = (item.findtext("title") or "").strip()
        desc  = (item.findtext("description") or "").strip()
        if keyword_filter:
            if not any(kw in (title + " " + desc).lower() for kw in KEYWORDS):
                continue
        items.append({
            "title":     title,
            "link":      _canonical(link),
            "published": (item.findtext("pubDate") or "").strip(),
            "source":    label,
        })
    return items


def _fetch_reddit(url: str) -> list[dict]:
    raw = _get(url)
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    items = []
    for child in data.get("data", {}).get("children", []):
        d    = child.get("data", {})
        link = d.get("url", "").strip()
        if not link:
            continue
        ts  = d.get("created_utc", 0)
        pub = (
            datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
            if ts else ""
        )
        items.append({
            "title":     d.get("title", "").strip(),
            "link":      _canonical(link),
            "published": pub,
            "source":    "Reddit",
        })
    return items


# ── Concurrent fetch ──────────────────────────────────────────────────────────

def fetch_all(seen: set[str]) -> list[dict]:
    """
    Fire all source requests in parallel, then apply the dedup pipeline:
      1. seen_items.json check (cross-run dedup)
      2. this-poll dedup set
      3. recency gate
    Returns only items that pass all three.
    """
    # Build task list
    # Each task: ("rss"|"reddit", url, label, keyword_filter_bool)
    tasks: list[tuple] = []

    for term in SEARCH_TERMS:
        enc = urllib.parse.quote(term)
        for tmpl, label in RSS_FEEDS:
            tasks.append(("rss", tmpl.format(query=enc), label, False))
        tasks.append(("reddit", REDDIT_URL.format(query=enc), "Reddit", None))

    for url, label in DIRECT_FEEDS:
        tasks.append(("rss", url, label, True))

    for ga_url in GOOGLE_ALERTS_FEEDS:
        tasks.append(("rss", ga_url, "Google Alerts", False))

    print(f"  [fetch] launching {len(tasks)} concurrent requests …")

    raw_items: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = []
        for kind, url, label, kw_filter in tasks:
            if kind == "rss":
                futures.append(pool.submit(_fetch_rss, url, label, kw_filter))
            else:
                futures.append(pool.submit(_fetch_reddit, url))
        for fut in as_completed(futures):
            try:
                raw_items.extend(fut.result())
            except Exception as exc:
                print(f"  [warn] worker exception: {exc}")

    print(f"  [fetch] {len(raw_items)} raw items across all sources")

    # Dedup pipeline (spec order):
    #  1. seen_items.json   2. this-poll set   3. recency gate
    this_poll: set[str] = set()
    results:   list[dict] = []
    for item in raw_items:
        link = item.get("link", "")
        if not link:
            continue
        if link in seen:                      # 1. cross-run dedup
            continue
        if link in this_poll:                 # 2. within-poll dedup
            continue
        this_poll.add(link)
        if _is_recent(item["published"], item["source"], item["title"]):  # 3. recency
            results.append(item)

    return results


# ── Email ─────────────────────────────────────────────────────────────────────

def _classify(item: dict) -> str:
    t = item["title"].lower()
    s = item["source"].lower()
    if "reddit"  in s: return "social media post"
    if any(w in t for w in ("press release", "announces", "statement", "launches")): return "press release"
    if any(w in t for w in ("interview", "op-ed", "opinion", "column", "commentary")): return "opinion piece"
    if any(w in t for w in ("podcast", "episode", "listen")): return "podcast mention"
    if "youtube" in s or "c-span" in s: return "video segment"
    return "media hit"


def _build_body(item: dict) -> str:
    now_str = datetime.now(timezone.utc).strftime("%B %d, %Y at %I:%M %p UTC")
    lines = [
        "Attention Secretary Sonderling,",
        "",
        f"A new {_classify(item)} has been published and your name is included.",
        "",
        f"Headline  : {item['title']}",
        f"Source    : {item['source']}",
        f"Posted at : {item['published'] or now_str}",
        f"Link      : {item['link']}",
        "",
        "─" * 60,
        "",
        f"— Sonderling Monitor  |  Alert generated {now_str}",
    ]
    return "\n".join(lines)


def send_alert(items: list[dict]) -> None:
    """One individual email per item — no batching. Sends to all ALERT_EMAIL recipients."""
    gmail_user = os.environ["GMAIL_USER"]
    recipients = [e.strip() for e in os.environ["ALERT_EMAIL"].split(",") if e.strip()]
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
        srv.login(gmail_user, os.environ["GMAIL_APP_PASSWORD"])
        for item in items:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"New Mention — Keith Sonderling: {item['title'][:80]}"
            msg["From"]    = f"Sonderling Monitor <{gmail_user}>"
            msg["To"]      = ", ".join(recipients)
            msg.attach(MIMEText(_build_body(item), "plain"))
            srv.sendmail(gmail_user, recipients, msg.as_string())
            print(f"  [alert] → {recipients}  [{item['source']}] {item['title'][:60]}")


# ── Poll loop ─────────────────────────────────────────────────────────────────

def poll_once(seen: set[str]) -> tuple[set[str], int]:
    new_items = fetch_all(seen)
    if new_items:
        send_alert(new_items)
        for it in new_items:
            seen.add(it["link"])
        save_seen(seen)
    return seen, len(new_items)


def main() -> None:
    seen   = load_seen()
    start  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=MAX_AGE_MINUTES)).strftime("%H:%M UTC")

    tier1 = len(RSS_FEEDS) * len(SEARCH_TERMS)
    reddit = len(SEARCH_TERMS)
    total  = tier1 + reddit + len(DIRECT_FEEDS) + len(GOOGLE_ALERTS_FEEDS)

    print(f"[start] {start}")
    print(f"[start] {len(seen)} previously seen items")
    print(f"[start] Recency gate: >{MAX_AGE_MINUTES} min old = dropped  (cutoff: {cutoff})")
    print(f"[start] {tier1} search feeds + {reddit} Reddit + {len(DIRECT_FEEDS)} direct outlets "
          f"+ {len(GOOGLE_ALERTS_FEEDS)} Google Alerts = {total} sources (all concurrent)")
    if not GOOGLE_ALERTS_FEEDS:
        print("[start] WARNING: GOOGLE_ALERTS_RSS not set — add GitHub secret for full-web coverage")
    print(f"[start] Poll every {POLL_INTERVAL}s for {LOOP_DURATION}s | {MAX_WORKERS} workers")

    deadline  = time.monotonic() + LOOP_DURATION
    total_new = 0

    while True:
        t0  = time.monotonic()
        print(f"\n[poll] {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")
        seen, n = poll_once(seen)
        total_new += n
        print(f"[poll] {n} new alert(s)  (run total: {total_new})")

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        sleep_for = max(0.0, min(POLL_INTERVAL - (time.monotonic() - t0), remaining))
        if sleep_for > 0:
            print(f"[poll] sleeping {sleep_for:.0f}s …")
            time.sleep(sleep_for)

    print(f"\n[done] {total_new} mention(s) alerted this run.")


if __name__ == "__main__":
    main()
