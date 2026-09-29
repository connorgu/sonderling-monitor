#!/usr/bin/env python3
"""
Keith Sonderling Media Monitor — 24/7 real-time alert system
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SOURCES: Google News RSS (×21 terms × 4 templates), 35 direct outlets,
         Bing Web p1+p2 (×21+8 terms), Bing News (×21+8 terms),
         Nitter/Twitter RSS (8 user timelines + 7 searches),
         Twitter API v2 (optional — set TWITTER_BEARER_TOKEN secret),
         Federal Register API
TWITTER: Nitter instances catch tweets within MINUTES of posting.
         Set TWITTER_BEARER_TOKEN for sub-second detection via API v2.
KEYWORDS: "Keith Sonderling" · "Sonderling" + context · Senate/cloture/confirmation
LATENCY:  Cron every 5 min → polls continuously for 4.5 min → ≤ 10 s worst case
"""
from __future__ import annotations
import base64, html as _html, json, os, re, smtplib, time, urllib.parse, urllib.request, xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime
from pathlib import Path

MAX_AGE_MINUTES     = 240  # RSS/Atom feeds — 4 hours to catch slow-indexed local/regional news
WEB_MAX_AGE_MINUTES = 480  # Bing Web — 8 hours; freshness=Day caps to 24h, this is the real floor

SEARCH_TERMS = [
    '"Sonderling"',                                # surname alone — catches ALL mentions incl. body-only
    '"Keith Sonderling"',                          # full name — primary precise hit
    '"Secretary Sonderling"',                      # how press refers to cabinet members
    '"Sonderling" "secretary of labor"',           # surname + formal title
    '"Sonderling" "labor secretary"',              # surname + common shorthand
    '"Keith Sonderling" "secretary of labor"',     # full name + formal title
    '"Keith Sonderling" "labor secretary"',        # full name + common shorthand
    '"Sonderling" "department of labor"',          # surname + department name
    '"Keith Sonderling" "department of labor"',    # full name + department name
    '"Keith Sonderling" "DOL"',                    # full name + acronym
    '"Sonderling" "DOL"',                          # surname + acronym
    '"Sonderling47"',                              # his X/Twitter handle — news coverage
    '"acting labor secretary" "Sonderling"',       # catches titles like KCTV5's subtitle
    '"acting secretary of labor" "Sonderling"',    # alternate formal phrasing
    # Senate confirmation tracking — catches breaking vote/cloture tweets fast
    '"Sonderling" "cloture"',                      # CraigCaplan-style cloture tweets
    '"Sonderling" "senate"',                       # any Senate floor activity
    '"Sonderling" "confirmation"',                 # confirmation vote news
    '"Sonderling" "nomination"',                   # nomination status updates
    '"Sonderling" "confirmed"',                    # confirmed as Secretary
    '"Sonderling" "vote"',                         # floor vote coverage
    '"Sonderling" "hearing"',                      # committee hearing references
]

# Matched against title+description for direct outlet feeds (Tier 3).
# "sonderling" MUST appear — blocks all generic DOL/labor content.
KEYWORDS = ["sonderling"]

# DOL.gov and White House may announce the Secretary by title without spelling the surname.
DOL_HIGH_VALUE_LABELS = {"Dept of Labor", "White House", "DOL YouTube", "White House YouTube"}
DOL_EXTRA_KEYWORDS    = ["secretary of labor", "labor secretary"]

RSS_FEEDS = [
    # Google News — three time windows: all-time (default), last 6 h, last 2 days.
    # The 6h window surfaces breaking hits first; 2d is the safety net for slow-indexed outlets.
    ("https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en",          "Google News"),
    ("https://news.google.com/rss/search?q={query}+when:6h&hl=en-US&gl=US&ceid=US:en",  "Google News 6h"),
    ("https://news.google.com/rss/search?q={query}+when:2d&hl=en-US&gl=US&ceid=US:en",  "Google News 2d"),
    # Yahoo News search RSS — entirely separate crawl/index, worth a parallel sweep.
    ("https://news.yahoo.com/rss/search?p={query}", "Yahoo News Search"),
    # Bing News RSS returns HTML bot-detection from GitHub Actions IPs (invalid XML) — removed
]

DIRECT_FEEDS = [
    # ── Major politics / news ─────────────────────────────────────────────────
    ("https://feeds.foxnews.com/foxnews/politics",               "Fox News"),
    ("https://feeds.foxnews.com/foxnews/latest",                 "Fox News (all)"),
    ("https://feeds.foxbusiness.com/foxbusiness/latest",         "Fox Business"),
    ("https://feeds.nbcnews.com/nbcnews/public/politics",        "NBC News"),
    ("https://abcnews.go.com/abcnews/politicsheadlines",         "ABC News"),
    ("https://www.cbsnews.com/latest/rss/politics",              "CBS News"),
    ("https://feeds.washingtonpost.com/rss/politics",            "Washington Post"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml","NY Times"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml","NY Times Business"),
    ("https://feeds.a.dj.com/rss/RSSWorldNews.xml",              "Wall Street Journal"),
    ("https://thehill.com/feed/",                                "The Hill"),
    ("https://www.npr.org/rss/rss.php?id=1014",                  "NPR Politics"),
    ("https://www.axios.com/feeds/feed.rss",                     "Axios"),
    ("https://www.breitbart.com/feed/",                          "Breitbart"),
    ("https://justthenews.com/feed",                             "Just The News"),
    ("https://www.nationalreview.com/feed/",                     "National Review"),
    ("https://spectator.org/feed/",                              "The Spectator"),
    ("https://www.realclearpolitics.com/index.xml",              "RealClearPolitics"),
    ("https://nypost.com/feed/",                                 "NY Post"),
    ("https://news.yahoo.com/rss/",                              "Yahoo News"),
    ("https://finance.yahoo.com/news/rssindex",                  "Yahoo Finance"),
    # ── Government / YouTube (Atom feeds — handled by updated _fetch_rss) ───────
    # White House RSS: all /feed/ paths return 404 from GitHub Actions; covered via Google News
    # DOL RSS returns 403 from GitHub IPs — covered by Google News search terms instead
    # YouTube Atom feeds work fine from GitHub Actions IPs:
    ("https://www.youtube.com/feeds/videos.xml?user=USDepartmentofLabor", "DOL YouTube"),
    ("https://www.youtube.com/feeds/videos.xml?user=whitehouse",           "White House YouTube"),
    # ── Additional outlets ────────────────────────────────────────────────────
    ("https://feeds.bloomberg.com/politics/news.rss",            "Bloomberg Politics"),
    ("https://www.cnbc.com/id/100003114/device/rss/rss.html",    "CNBC"),
    ("https://www.cnbc.com/id/10000664/device/rss/rss.html",     "CNBC Economy"),
    ("https://dailycaller.com/feed/",                            "Daily Caller"),
    ("https://www.washingtonexaminer.com/feed",                  "Washington Examiner"),
    ("https://www.dailywire.com/feeds/rss.xml",                  "Daily Wire"),
    ("https://thefederalist.com/feed/",                          "The Federalist"),
    # Newsweek /rss → 404 from GitHub Actions — covered by Google News search terms
    # ── Labor / HR specialty ─────────────────────────────────────────────────────
    ("https://news.bloomberglaw.com/rss/daily-labor-report",    "Bloomberg Daily Labor"),
    ("https://www.hrdive.com/feeds/news/",                      "HR Dive"),
    ("https://ogletree.com/feed/",                              "Ogletree Deakins"),
    # ── Additional working outlets (confirmed from local tests) ──────────────────
    ("https://rollcall.com/feed/",                              "Roll Call"),
    ("https://rss.politico.com/politics-news.xml",              "Politico"),
    # Removed (confirmed dead/blocked from GitHub Actions IPs):
    # Reuters feeds.reuters.com — DNS failure (Name or service not known)
    # CNN rss.cnn.com — SSL EOF error
    # Politico /rss/politicopicks.xml — 403 Forbidden
    # AP News apnews.com/rss — 403 Forbidden
    # DOL dol.gov/rss/releases.xml — 403 Forbidden (covered via Google News)
    # BLS bls.gov/feed/bls_latest.rss — 403 Forbidden
    # Congress congress.gov/rss — 404 Not Found
    # Yahoo Politics /rss/politics — 404 Not Found
    # Washington Times — 403 Forbidden
    # C-SPAN /feeds/clips.rss — 404 Not Found
    # Newsmax /rss/Politics/16/ — read timeout
]

_GA_RAW = os.environ.get("GOOGLE_ALERTS_RSS", "").strip()
GOOGLE_ALERTS_FEEDS: list[str] = [u.strip() for u in _GA_RAW.split(",") if u.strip()]

# Reddit JSON API returns 403 from GitHub Actions IPs on every request — removed
# DuckDuckGo HTML returns 403/connection-drop from GitHub Actions IPs — removed
BING_WEB_URL  = "https://www.bing.com/search?q={query}&setlang=en&cc=US&first={first}&freshness=Day"
BING_NEWS_URL = "https://www.bing.com/news/search?q={query}&setlang=en&cc=US&sortby=Date"

# Extra queries aimed at social platforms — indexed by Bing within ~60 min
SOCIAL_SEARCH_TERMS = [
    '"Keith Sonderling" site:linkedin.com',      # LinkedIn posts/articles
    '"Keith Sonderling" site:twitter.com',       # Twitter (older indexed posts)
    '"Keith Sonderling" site:x.com',             # X.com posts
    '"Sonderling47" site:x.com',                 # his @handle — catches replies/tags
    '"Sonderling" site:x.com',                   # surname alone on X — catches body mentions
    '"Sonderling47"',                             # handle as standalone Bing search (not site:)
    '"Keith Sonderling" site:youtube.com',       # YouTube videos mentioning him
    # Senate reporters who break floor/cloture news — site: searches pick up fast
    '"Sonderling" site:c-span.org',              # C-SPAN coverage
]

# Nitter — open-source Twitter frontend with RSS support; real-time, no API key.
# Tries each instance in order; falls back to next on failure.
# These catch tweets MINUTES after posting — 30-60x faster than Bing indexing.
NITTER_INSTANCES = [
    "nitter.privacydev.net",
    "nitter.net",
    "nitter.1d4.us",
    "nitter.poast.org",
    "nitter.catsarch.com",
]

# High-value Twitter accounts to monitor directly via Nitter user-timeline RSS.
# These are the journalists and officials who break Sonderling news first.
NITTER_ACCOUNTS = [
    ("Sonderling47",    False),  # Sonderling himself — all his posts are relevant
    ("CraigCaplan",     True),   # C-SPAN Capitol Hill reporter — broke cloture tweet
    ("sahilkapur",      True),   # NBC News Congress reporter
    ("frankthorp",      True),   # NBC News Capitol Hill
    ("DOL",             True),   # Dept of Labor official account
    ("WhiteHouseGov",   True),   # White House announcements
    ("SenateGOP",       True),   # Senate Republican caucus
    ("POTUS",           True),   # Presidential announcements
]

# Twitter search queries via Nitter — real-time Twitter search, no auth needed
NITTER_SEARCH_TERMS = [
    '"Keith Sonderling"',
    '"Secretary Sonderling"',
    '"Sonderling" labor',
    '"Sonderling" cloture',
    '"Sonderling" confirmation',
    '"Sonderling" senate',
    'Sonderling47',
]

# Optional: Twitter API v2 Essential (free tier) — 15 req/15min, real-time.
# Set TWITTER_BEARER_TOKEN as a GitHub Actions secret for instant tweet detection.
TWITTER_BEARER = os.environ.get("TWITTER_BEARER_TOKEN", "")
_last_twitter_v2: float = 0.0

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
MAX_WORKERS   = int(os.environ.get("MAX_WORKERS",   "100"))

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
        # RFC 2822 first (standard RSS), then ISO 8601 (Atom / some feeds)
        try:
            pub_dt = parsedate_to_datetime(pub_str)
        except Exception:
            pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
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

def load_seen() -> dict[str, str]:
    if SEEN_FILE.exists():
        try:
            data = json.loads(SEEN_FILE.read_text())
            if isinstance(data, list):
                now_iso = datetime.now(timezone.utc).isoformat()
                return {url: now_iso for url in data if isinstance(url, str)}
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    return {}


def save_seen(seen: dict[str, str]) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
    pruned = {url: ts for url, ts in seen.items() if ts >= cutoff}
    if len(pruned) > 5000:
        pruned = dict(sorted(pruned.items(), key=lambda x: x[1])[-5000:])
    SEEN_FILE.write_text(json.dumps(pruned, indent=2))


def _sync_seen_from_github() -> None:
    """On Railway startup, pull seen_items.json from GitHub to restore dedup state."""
    token = os.environ.get("GH_TOKEN", "")
    if not token:
        return
    try:
        url = "https://api.github.com/repos/connorgu/sonderling-monitor/contents/seen_items.json"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "SonderlingMonitor/1.0",
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        content = base64.b64decode(data["content"]).decode()
        SEEN_FILE.write_text(content)
        print(f"[startup] Synced seen_items.json from GitHub ({len(json.loads(content))} entries)")
    except Exception as exc:
        print(f"[startup] Could not sync seen_items.json from GitHub: {exc} — starting fresh")


def _push_seen_to_github(seen: dict[str, str]) -> None:
    """Push seen_items.json to GitHub after each alert (Railway mode dedup persistence)."""
    token = os.environ.get("GH_TOKEN", "")
    if not token:
        return
    try:
        url = "https://api.github.com/repos/connorgu/sonderling-monitor/contents/seen_items.json"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "SonderlingMonitor/1.0",
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            current = json.loads(r.read())
        sha = current.get("sha", "")
        content_b64 = base64.b64encode(SEEN_FILE.read_bytes()).decode()
        body = json.dumps({
            "message": "chore: update seen items [skip ci]",
            "content": content_b64,
            "sha": sha,
        }).encode()
        req2 = urllib.request.Request(url, data=body, method="PUT", headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "SonderlingMonitor/1.0",
        })
        with urllib.request.urlopen(req2, timeout=10) as r2:
            r2.read()
        print(f"  [sync] Pushed seen_items.json to GitHub")
    except Exception as exc:
        print(f"  [sync] Could not push seen_items.json to GitHub: {exc}")


# ── Snippet date extractor ────────────────────────────────────────────────────

def _parse_snippet_date(text: str) -> str | None:
    """
    Extract a real publication date from web search result text.
    DDG and Bing both show age indicators like '4 minutes ago', '2 hours ago',
    or absolute dates like 'Sep 15, 2026' directly in their HTML results.
    Returns an RFC-2822 date string if found, None otherwise.
    """
    now = datetime.now(timezone.utc)

    # ISO datetime: 2026-09-15T13:45:00Z or 2026-09-15 13:45
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})?', text)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                          int(m.group(4)), int(m.group(5)), tzinfo=timezone.utc)
            return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")
        except Exception:
            pass

    # ISO date-only: 2026-09-15. If it's today, use now (avoids midnight-UTC false-drops).
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})(?!\d)', text)
    if m:
        try:
            article_date = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
            today = now.replace(hour=0, minute=0, second=0, microsecond=0)
            dt = now if article_date >= today else article_date
            return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")
        except Exception:
            pass

    m = re.search(r'(\d+)\s+minute[s]?\s+ago', text, re.IGNORECASE)
    if m:
        return (now - timedelta(minutes=int(m.group(1)))).strftime("%a, %d %b %Y %H:%M:%S +0000")

    m = re.search(r'(\d+)\s+hour[s]?\s+ago', text, re.IGNORECASE)
    if m:
        return (now - timedelta(hours=int(m.group(1)))).strftime("%a, %d %b %Y %H:%M:%S +0000")

    m = re.search(r'(\d+)\s+day[s]?\s+ago', text, re.IGNORECASE)
    if m:
        return (now - timedelta(days=int(m.group(1)))).strftime("%a, %d %b %Y %H:%M:%S +0000")

    # "Sep 15, 2026" / "September 15, 2026" / "15 Sep 2026"
    m = re.search(
        r'(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|'
        r'Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
        r'\.?\s+(\d{1,2}),?\s+(\d{4})',
        text, re.IGNORECASE,
    )
    if m:
        try:
            dt = datetime.strptime(
                f"{m.group(1)[:3].capitalize()} {int(m.group(2)):02d} {m.group(3)}", "%b %d %Y"
            )
            return dt.replace(tzinfo=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
        except Exception:
            pass

    return None


# ── Twitter/X snowflake date decoder ─────────────────────────────────────────

def _tweet_age_minutes(url: str) -> float | None:
    """
    Twitter encodes the exact publish timestamp inside every tweet ID (snowflake).
    This lets us verify age with 100% accuracy — no guessing, no search engine claims.
    Returns age in minutes, or None if URL is not a tweet.
    """
    m = re.search(r'(?:twitter\.com|x\.com)/\w+/status(?:es)?/(\d+)', url)
    if not m:
        return None
    try:
        tweet_id = int(m.group(1))
        ts_ms  = (tweet_id >> 22) + 1288834974657   # Twitter epoch offset
        pub_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return (datetime.now(timezone.utc) - pub_dt).total_seconds() / 60
    except Exception:
        return None


# ── Network ───────────────────────────────────────────────────────────────────

def _get(url: str, timeout: int = 15, retry: bool = True) -> bytes | None:
    attempts = 2 if retry else 1
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as exc:
            if attempt == attempts - 1:
                print(f"  [warn] {url[:80]}  →  {exc}")
            else:
                time.sleep(3)
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
        netloc = parsed.netloc.lower().removeprefix("www.")
        return urllib.parse.urlunparse((
            parsed.scheme.lower(),
            netloc,
            parsed.path.rstrip("/"),
            parsed.params,
            urllib.parse.urlencode(qs, doseq=True),
            "",
        ))
    except Exception:
        return url


def _decode_bing_ck_url(url: str) -> str:
    """Bing wraps every web result in /ck/a?...u=a1BASE64URL. Decode to the real URL."""
    try:
        params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        encoded = (params.get("u") or [""])[0]
        if encoded.startswith("a1"):
            decoded = base64.urlsafe_b64decode(encoded[2:] + "==").decode("utf-8", errors="replace")
            if decoded.startswith("http") and "bing.com" not in decoded:
                return decoded
    except Exception:
        pass
    return url


# ── Per-feed fetch workers (called concurrently) ──────────────────────────────

_ATOM  = "{http://www.w3.org/2005/Atom}"    # YouTube / gov Atom feeds
_MEDIA = "{http://search.yahoo.com/mrss/}"  # YouTube media namespace


def _fetch_rss(url: str, label: str, keyword_filter: bool) -> list[dict]:
    """Parse RSS 2.0 and Atom feeds (YouTube uses Atom with media: extensions)."""
    raw = _get(url)
    if not raw:
        return []
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        print(f"  [warn] parse error ({label}): {exc}")
        return []

    def _kw_ok(title: str, desc: str) -> bool:
        txt = (title + " " + desc).lower()
        if any(kw in txt for kw in KEYWORDS):
            return True
        if label in DOL_HIGH_VALUE_LABELS:
            return any(kw in txt for kw in DOL_EXTRA_KEYWORDS)
        return False

    items: list[dict] = []

    # ── RSS 2.0 path ──────────────────────────────────────────────────────────
    rss_entries = root.findall(".//item")
    if rss_entries:
        for el in rss_entries:
            link  = (el.findtext("link") or "").strip()
            if not link:
                continue
            title = (el.findtext("title") or "").strip()
            desc  = (el.findtext("description") or "").strip()
            if keyword_filter and not _kw_ok(title, desc):
                continue
            items.append({
                "title":     title,
                "link":      _canonical(link),
                "published": (el.findtext("pubDate") or "").strip(),
                "source":    label,
            })
        return items

    # ── Atom path (YouTube, some gov feeds) ──────────────────────────────────
    atom_entries = root.findall(f".//{_ATOM}entry") or root.findall(".//entry")
    for el in atom_entries:
        title = (el.findtext(f"{_ATOM}title") or el.findtext("title") or "").strip()
        # Atom link is an attribute, not text
        link_el = el.find(f"{_ATOM}link") or el.find("link")
        link = (link_el.get("href") if link_el is not None else "") or ""
        # YouTube fallback: decode yt:video:ID from <id>
        if not link:
            id_val = (el.findtext(f"{_ATOM}id") or el.findtext("id") or "").strip()
            if "yt:video:" in id_val:
                link = "https://www.youtube.com/watch?v=" + id_val.split("yt:video:")[-1]
        if not link:
            continue
        pub = (el.findtext(f"{_ATOM}published") or el.findtext("published") or
               el.findtext(f"{_ATOM}updated")   or el.findtext("updated") or "").strip()
        # YouTube description is inside <media:group><media:description>
        grp  = el.find(f"{_MEDIA}group")
        desc = (grp.findtext(f"{_MEDIA}description") if grp is not None else None) or \
               el.findtext(f"{_ATOM}summary") or el.findtext("summary") or ""
        if keyword_filter and not _kw_ok(title, desc):
            continue
        items.append({
            "title":     title,
            "link":      _canonical(link),
            "published": pub,
            "source":    label,
        })
    return items



def _fetch_federal_register() -> list[dict]:
    """
    Free Federal Register API — no auth required.
    Catches every DOL rule, executive order, and notice mentioning Sonderling.
    publication_date is date-only; treat same-day results as current.
    """
    url = (
        "https://www.federalregister.gov/api/v1/articles.json"
        "?conditions%5Bterm%5D=Sonderling&order=newest&per_page=20"
        "&fields%5B%5D=title&fields%5B%5D=html_url&fields%5B%5D=publication_date"
        "&fields%5B%5D=abstract&fields%5B%5D=type"
    )
    raw = _get(url)
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    items = []
    for doc in data.get("results", []):
        link = (doc.get("html_url") or "").strip()
        if not link:
            continue
        title = (doc.get("title") or "").strip()
        pub_date = (doc.get("publication_date") or "").strip()
        abstract = (doc.get("abstract") or "").strip()
        if not pub_date:
            continue
        # Only alert on today's Federal Register (it publishes once per business day)
        if pub_date != today_str:
            continue
        pub_str = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
        items.append({
            "title":     title or f"Federal Register: {doc.get('type', 'Document')}",
            "link":      _canonical(link),
            "published": pub_str,
            "source":    "Federal Register",
            "snippet":   abstract[:200] if abstract else "",
        })
    return items


def _fetch_web_search(query: str, label: str, first: int = 1) -> list[dict]:
    """
    Scrape Bing Web search results for recent Sonderling mentions.
    `first` controls pagination: 1=page1, 11=page2, 21=page3 (Bing shows ~10 results/page).

    DATE POLICY — three-tier, strict:
      1. Twitter/X: snowflake ID decoded to exact minute — drop if > MAX_AGE_MINUTES.
      2. Other URLs: parse date from Bing news_dt element or snippet text.
         Drop if > WEB_MAX_AGE_MINUTES.
      3. No date recoverable at all: DROP.  Never assume "now" for unknown-age content.
    """
    url = BING_WEB_URL.format(query=urllib.parse.quote(query), first=first)

    raw = _get(url)
    if not raw:
        return []

    items: list[dict] = []
    text = raw.decode("utf-8", errors="replace")

    def _clean(html_frag: str) -> str:
        return _html.unescape(re.sub(r"<[^>]+>", "", html_frag)).strip()

    try:
        # Bing web structure:
        #   <h2><a href="https://www.bing.com/ck/a?...u=a1BASE64URL...">Title</a></h2>
        #   <div class="b_caption"><p><span class="news_dt">4 min ago</span> snippet</p></div>
        # All hrefs are bing.com/ck/a? redirects — must decode to get the real URL.
        pairs = re.findall(
            r'<h2[^>]*>\s*<a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>\s*</h2>'
            r'.*?<div[^>]+class="b_caption"[^>]*>.*?<p[^>]*>(.*?)</p>',
            text, re.DOTALL,
        )
        for href_raw, title_raw, snip_raw in pairs[:30]:
            href = _decode_bing_ck_url(href_raw) if "bing.com/ck/a" in href_raw else href_raw
            if not href.startswith("http") or "bing.com" in href:
                continue
            title    = _clean(title_raw)
            snip_raw_clean = snip_raw
            snip     = _clean(snip_raw)
            combined = (title + " " + snip).lower()
            if not any(kw in combined for kw in KEYWORDS) and \
               "secretary of labor" not in combined and "labor secretary" not in combined:
                continue

            # ── DATE VERIFICATION ─────────────────────────────────────────
            tweet_age = _tweet_age_minutes(href)
            if tweet_age is not None:
                if tweet_age > MAX_AGE_MINUTES:
                    print(f"    [drop-old-tweet {tweet_age:.0f}m] {title[:60]}")
                    continue
                pub = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

            else:
                dt_span = re.search(r'class="news_dt"[^>]*>(.*?)</span>', snip_raw_clean, re.DOTALL)
                dt_text = _clean(dt_span.group(1)) if dt_span else ""
                pub = _parse_snippet_date(dt_text) or _parse_snippet_date(snip)
                if not pub:
                    print(f"    [drop-nodate-web] {label}: {title[:60]}")
                    continue
                from email.utils import parsedate_to_datetime as _p2dt
                try:
                    _age = (datetime.now(timezone.utc) - _p2dt(pub)).total_seconds() / 60
                    if _age > WEB_MAX_AGE_MINUTES:
                        print(f"    [drop-old-web {_age:.0f}m] {label}: {title[:60]}")
                        continue
                except Exception:
                    pass

            items.append({
                "title": title, "link": _canonical(href),
                "published": pub, "source": label, "snippet": snip,
            })

    except Exception as exc:
        print(f"  [warn] web-search parse ({label}): {exc}")

    return items


def _fetch_bing_news(query: str, label: str) -> list[dict]:
    """
    Scrape Bing News tab (news.bing.com, date-sorted) — separate crawler from Bing Web.
    Bing News indexes breaking news faster and surfaces local/regional outlets that
    Bing Web sometimes buries on page 3+.
    Same date-verification logic as _fetch_web_search.
    """
    raw = _get(BING_NEWS_URL.format(query=urllib.parse.quote(query)))
    if not raw:
        return []
    text = raw.decode("utf-8", errors="replace")
    items: list[dict] = []

    def _clean(s: str) -> str:
        return _html.unescape(re.sub(r"<[^>]+>", "", s)).strip()

    try:
        # Bing News card structure:
        #   <a class="title" href="URL">Title</a>
        #   followed within ~800 chars by snippet text and a timestamp like "2 hours ago"
        hits = re.findall(
            r'<a\s[^>]*\bclass="[^"]*\btitle\b[^"]*"[^>]*\bhref="([^"]+)"[^>]*>(.*?)</a>'
            r'(.{0,900}?)(?=<a\s[^>]*\bclass="[^"]*\btitle\b|</body>|$)',
            text, re.DOTALL,
        )
        for href_raw, title_raw, context in hits[:40]:
            href = _decode_bing_ck_url(href_raw) if "bing.com/ck/a" in href_raw else href_raw
            if not href.startswith("http") or "bing.com" in href or "microsoft.com" in href:
                continue
            title = _clean(title_raw)
            if not title or len(title) < 6:
                continue
            ctx = _clean(context)
            combined = (title + " " + ctx).lower()
            if not any(kw in combined for kw in KEYWORDS) and \
               "secretary of labor" not in combined and "labor secretary" not in combined:
                continue

            tweet_age = _tweet_age_minutes(href)
            if tweet_age is not None:
                if tweet_age > MAX_AGE_MINUTES:
                    print(f"    [drop-old-tweet {tweet_age:.0f}m] {title[:60]}")
                    continue
                pub = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
            else:
                pub = _parse_snippet_date(ctx) or _parse_snippet_date(title)
                if not pub:
                    print(f"    [drop-nodate-bnews] {label}: {title[:60]}")
                    continue
                try:
                    age_m = (datetime.now(timezone.utc) - parsedate_to_datetime(pub)).total_seconds() / 60
                    if age_m > WEB_MAX_AGE_MINUTES:
                        print(f"    [drop-old-bnews {age_m:.0f}m] {label}: {title[:60]}")
                        continue
                except Exception:
                    pass

            items.append({
                "title": title, "link": _canonical(href),
                "published": pub, "source": label, "snippet": ctx[:200],
            })
    except Exception as exc:
        print(f"  [warn] bing-news ({label}): {exc}")
    return items


def _fetch_nitter(path: str, label: str, keyword_filter: bool) -> list[dict]:
    """
    Real-time Twitter via Nitter RSS — no API key, no rate limit.
    Tries each instance in NITTER_INSTANCES until one returns valid XML.
    `path` is either "{username}/rss" or "search/rss?q={query}&f=tweets".
    Returns results minutes after posting — not 30-60 min Bing-indexing lag.
    """
    for inst in NITTER_INSTANCES:
        url = f"https://{inst}/{path}"
        raw = _get(url, timeout=5, retry=False)
        if not raw:
            continue
        head = raw[:500]
        if b"<rss" not in head and b"<?xml" not in head and b"<feed" not in head:
            continue
        try:
            root = ET.fromstring(raw)
        except ET.ParseError:
            continue

        items: list[dict] = []
        for el in root.findall(".//item"):
            link  = (el.findtext("link") or "").strip()
            if not link:
                continue
            title = (el.findtext("title") or "").strip()
            desc  = (el.findtext("description") or "").strip()
            if keyword_filter:
                txt = (title + " " + re.sub(r"<[^>]+>", "", desc)).lower()
                if not any(kw in txt for kw in KEYWORDS) and \
                   "secretary of labor" not in txt and "labor secretary" not in txt:
                    continue
            pub = (el.findtext("pubDate") or "").strip()
            # Nitter sometimes puts x.com links; normalize to x.com canonical
            link = link.replace(f"{inst}/", "x.com/").replace("https://nitter.", "https://x.")
            items.append({
                "title":     title,
                "link":      _canonical(link),
                "published": pub,
                "source":    label,
                "snippet":   re.sub(r"<[^>]+>", "", _html.unescape(desc))[:200],
            })
        return items

    return []


def _fetch_twitter_v2() -> list[dict]:
    """
    Twitter API v2 recent search — free Essential tier, up to 100 tweets/call.
    Enforces a 60-second minimum between calls to respect 15 req/15 min rate limit.
    Set TWITTER_BEARER_TOKEN as a GitHub Actions secret to enable this.
    Catches tweets within seconds of posting — best possible Twitter latency.
    """
    global _last_twitter_v2
    if not TWITTER_BEARER:
        return []
    now = time.monotonic()
    if now - _last_twitter_v2 < 60:
        return []
    _last_twitter_v2 = now

    # Combined OR query: one API call covers all key variations
    query = (
        '("Keith Sonderling" OR "Secretary Sonderling" OR '
        '"Sonderling47" OR "Sonderling labor" OR "Sonderling cloture" OR '
        '"Sonderling senate" OR "Sonderling confirmation") lang:en -is:retweet'
    )
    url = (
        "https://api.twitter.com/2/tweets/search/recent"
        "?query=" + urllib.parse.quote(query) +
        "&max_results=100"
        "&tweet.fields=created_at,text,author_id"
        "&expansions=author_id"
        "&user.fields=username"
        "&sort_order=recency"
    )
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {TWITTER_BEARER}",
        "User-Agent":    "SonderlingMonitor/1.0",
        "Accept":        "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
    except Exception as exc:
        print(f"  [warn] Twitter API v2: {exc}")
        return []

    users = {u["id"]: u["username"] for u in data.get("includes", {}).get("users", [])}
    items: list[dict] = []
    for tw in (data.get("data") or []):
        username = users.get(tw.get("author_id", ""), "unknown")
        link = f"https://x.com/{username}/status/{tw['id']}"

        age = _tweet_age_minutes(link)
        if age is not None and age > MAX_AGE_MINUTES:
            continue

        raw_dt = tw.get("created_at", "")
        try:
            pub = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")).strftime(
                "%a, %d %b %Y %H:%M:%S +0000")
        except Exception:
            pub = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

        text_body = tw.get("text", "")
        items.append({
            "title":     f"@{username}: {text_body[:120]}",
            "link":      _canonical(link),
            "published": pub,
            "source":    "Twitter API",
            "snippet":   text_body[:300],
        })
    return items


# ── Concurrent fetch ──────────────────────────────────────────────────────────

def fetch_all(seen: dict[str, str]) -> list[dict]:
    """
    Fire all source requests in parallel, then apply the dedup pipeline:
      1. seen_items.json check (cross-run dedup)
      2. this-poll dedup set
      3. recency gate
    Returns only items that pass all three.
    """
    # Build task list — ("rss"|"web"|"bnews"|"fedreg", url_or_query, label, aux)
    # aux: keyword_filter bool for rss; page-start int for web; None otherwise
    tasks: list[tuple] = []

    for term in SEARCH_TERMS:
        enc = urllib.parse.quote(term)
        for tmpl, label in RSS_FEEDS:
            # keyword_filter=False: the search query already targets Sonderling,
            # so re-checking the snippet would drop articles where his name is
            # only in the body (e.g. "High fuel costs" article, KCTV5 Sept 28).
            tasks.append(("rss", tmpl.format(query=enc), label, False))

    for url, label in DIRECT_FEEDS:
        tasks.append(("rss", url, label, True))

    for ga_url in GOOGLE_ALERTS_FEEDS:
        tasks.append(("rss", ga_url, "Google Alerts", False))

    # Bing Web page 1 + page 2 — 10 results/page; page 2 catches hits buried by fresher content
    for term in SEARCH_TERMS:
        tasks.append(("web",  term, "Bing Web",    1))   # results 1-10
        tasks.append(("web",  term, "Bing Web p2", 11))  # results 11-20
    for term in SOCIAL_SEARCH_TERMS:
        tasks.append(("web",  term, "Bing Web",    1))
        tasks.append(("web",  term, "Bing Web p2", 11))
    # Bing News tab — separate crawler/index, date-sorted, surfaces local TV news faster
    for term in SEARCH_TERMS:
        tasks.append(("bnews", term, "Bing News", None))
    for term in SOCIAL_SEARCH_TERMS:
        tasks.append(("bnews", term, "Bing News", None))
    # Federal Register API — free, no auth, covers DOL rules/notices/executive orders
    tasks.append(("fedreg", None, "Federal Register", None))
    # ── Nitter/Twitter real-time RSS ──────────────────────────────────────────
    # User timelines — catches tweets from journalists/officials within minutes
    for username, kw_filter in NITTER_ACCOUNTS:
        tasks.append(("nitter", f"{username}/rss", f"Twitter/@{username}", kw_filter))
    # Twitter search via Nitter — real-time keyword search, no indexing lag
    for q in NITTER_SEARCH_TERMS:
        enc = urllib.parse.quote(q)
        tasks.append(("nitter", f"search/rss?q={enc}&f=tweets", "Twitter Search", False))
    # Twitter API v2 (optional — set TWITTER_BEARER_TOKEN GitHub secret for fastest alerts)
    tasks.append(("twitter_v2", None, "Twitter API v2", None))

    print(f"  [fetch] launching {len(tasks)} concurrent requests …")

    raw_items: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = []
        for kind, url, label, aux in tasks:
            if kind == "rss":
                futures.append(pool.submit(_fetch_rss, url, label, aux))
            elif kind == "fedreg":
                futures.append(pool.submit(_fetch_federal_register))
            elif kind == "bnews":
                futures.append(pool.submit(_fetch_bing_news, url, label))
            elif kind == "nitter":
                futures.append(pool.submit(_fetch_nitter, url, label, aux if aux is not None else True))
            elif kind == "twitter_v2":
                futures.append(pool.submit(_fetch_twitter_v2))
            else:  # web — aux is the `first` page offset (1 or 11)
                futures.append(pool.submit(_fetch_web_search, url, label, aux or 1))
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
    l = item.get("link", "").lower()
    if "linkedin.com" in l:                                                    return "LinkedIn post"
    if "twitter.com" in l or "x.com" in l:                                    return "X (Twitter) post"
    if "reddit" in s or "reddit.com" in l:                                     return "Reddit post"
    if "youtube" in l or "c-span" in s:                                        return "video segment"
    if any(w in t for w in ("press release", "announces", "statement")):       return "press release"
    if any(w in t for w in ("interview", "op-ed", "opinion", "column")):       return "opinion piece"
    if any(w in t for w in ("podcast", "episode", "listen")):                  return "podcast mention"
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
    ]
    snippet = item.get("snippet", "").strip()
    if snippet:
        lines += ["", f"Preview   : {snippet[:300]}"]
    lines += [
        "",
        "─" * 60,
        "",
        f"— Sonderling Monitor  |  Alert generated {now_str}",
    ]
    return "\n".join(lines)


def _build_html_body(item: dict) -> str:
    now_str = datetime.now(timezone.utc).strftime("%B %d, %Y at %I:%M %p UTC")
    kind = _classify(item)
    snippet = _html.escape(item.get("snippet", "").strip()[:300])
    title_esc = _html.escape(item["title"])
    source_esc = _html.escape(item["source"])
    pub_esc = _html.escape(item["published"] or now_str)
    link = item["link"]
    snip_row = f'<tr><td style="color:#6b7280;padding:4px 0"><b>Preview</b></td><td style="padding:4px 0 4px 12px">{snippet}</td></tr>' if snippet else ""
    return f"""<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
  <div style="background:#1e3a5f;color:#fff;padding:16px 20px;border-radius:6px 6px 0 0">
    <span style="font-size:11px;letter-spacing:1px;opacity:.75">SONDERLING MONITOR</span>
    <h2 style="margin:4px 0 0;font-size:16px">{title_esc}</h2>
  </div>
  <div style="background:#f9fafb;padding:16px 20px;border:1px solid #e5e7eb;border-top:none">
    <table style="width:100%;border-collapse:collapse;font-size:14px">
      <tr><td style="color:#6b7280;padding:4px 0;white-space:nowrap"><b>Type</b></td><td style="padding:4px 0 4px 12px">{kind}</td></tr>
      <tr><td style="color:#6b7280;padding:4px 0"><b>Source</b></td><td style="padding:4px 0 4px 12px">{source_esc}</td></tr>
      <tr><td style="color:#6b7280;padding:4px 0"><b>Posted</b></td><td style="padding:4px 0 4px 12px">{pub_esc}</td></tr>
      <tr><td style="color:#6b7280;padding:4px 0"><b>Link</b></td><td style="padding:4px 0 4px 12px"><a href="{link}" style="color:#1e3a5f">{link[:80]}{"..." if len(link)>80 else ""}</a></td></tr>
      {snip_row}
    </table>
  </div>
  <div style="padding:10px 20px;background:#fff;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 6px 6px;font-size:11px;color:#9ca3af">
    Alert generated {now_str}
  </div>
</div>"""


def _subject_tag(item: dict) -> str:
    """Short prefix so the email source is visible before opening."""
    l = item.get("link", "").lower()
    s = item["source"].lower()
    if "linkedin.com"               in l: return "[LINKEDIN]"
    if "twitter.com" in l or "x.com" in l: return "[X/TWITTER]"
    if "reddit.com"  in l or "reddit" in s: return "[REDDIT]"
    if "google alerts"              in s: return "[GOOGLE ALERT]"
    if "bing web" in s: return "[WEB]"
    if "whitehouse.gov"             in l: return "[WHITE HOUSE]"
    if "dol.gov"                    in l: return "[DEPT OF LABOR]"
    if "congress.gov"               in l: return "[CONGRESS]"
    return f"[{item['source'].upper()[:14]}]"


def send_alert(items: list[dict]) -> None:
    """One individual email per item — no batching. Sends to all ALERT_EMAIL recipients."""
    gmail_user = os.environ["GMAIL_USER"]
    recipients = [e.strip() for e in os.environ["ALERT_EMAIL"].split(",") if e.strip()]
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
        srv.login(gmail_user, os.environ["GMAIL_APP_PASSWORD"])
        for item in items:
            tag = _subject_tag(item)
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"{tag} Keith Sonderling: {item['title'][:80]}"
            msg["From"]    = f"Sonderling Monitor <{gmail_user}>"
            msg["To"]      = ", ".join(recipients)
            msg.attach(MIMEText(_build_body(item), "plain"))
            msg.attach(MIMEText(_build_html_body(item), "html"))
            srv.sendmail(gmail_user, recipients, msg.as_string())
            print(f"  [alert] → {recipients}  [{item['source']}] {item['title'][:60]}")


# ── Poll loop ─────────────────────────────────────────────────────────────────

def poll_once(seen: dict[str, str]) -> tuple[dict[str, str], int]:
    new_items = fetch_all(seen)
    if new_items:
        # Mark seen BEFORE sending — prevents double-alert if SMTP fails mid-batch
        now_iso = datetime.now(timezone.utc).isoformat()
        for it in new_items:
            seen[it["link"]] = now_iso
        save_seen(seen)
        send_alert(new_items)
        if LOOP_DURATION == 0:
            _push_seen_to_github(seen)
    return seen, len(new_items)


def main() -> None:
    for key in ("GMAIL_USER", "GMAIL_APP_PASSWORD", "ALERT_EMAIL"):
        if not os.environ.get(key):
            raise RuntimeError(f"Required secret {key!r} is not set in environment")
    if LOOP_DURATION == 0 and not os.environ.get("GH_TOKEN"):
        print("[warn] GH_TOKEN not set — seen_items.json will not persist to GitHub across redeploys")
    if LOOP_DURATION == 0:
        _sync_seen_from_github()
    seen   = load_seen()
    start  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=MAX_AGE_MINUTES)).strftime("%H:%M UTC")

    n_terms  = len(SEARCH_TERMS)
    n_social = len(SOCIAL_SEARCH_TERMS)
    rss_tasks  = len(RSS_FEEDS) * n_terms
    web_tasks  = (n_terms + n_social) * 2   # page1 + page2
    news_tasks = (n_terms + n_social)        # Bing News tab
    total    = rss_tasks + len(DIRECT_FEEDS) + len(GOOGLE_ALERTS_FEEDS) + web_tasks + news_tasks + 1

    print(f"[start] {start}")
    print(f"[start] {len(seen)} previously seen items")
    print(f"[start] Recency gate: RSS >{MAX_AGE_MINUTES} min  |  Web >{WEB_MAX_AGE_MINUTES} min")
    print(f"[start] {rss_tasks} Google/Yahoo News feeds + {len(DIRECT_FEEDS)} direct outlets "
          f"+ {len(GOOGLE_ALERTS_FEEDS)} Google Alerts "
          f"+ {web_tasks} Bing Web (p1+p2) + {news_tasks} Bing News + 1 Fed Reg "
          f"= {total} parallel tasks")
    if not GOOGLE_ALERTS_FEEDS:
        print("[start] WARNING: GOOGLE_ALERTS_RSS not set — add GitHub secret for full-web coverage")
    mode_str = "∞ (Railway persistent)" if LOOP_DURATION == 0 else f"{LOOP_DURATION}s"
    print(f"[start] Poll every {POLL_INTERVAL}s for {mode_str} | {MAX_WORKERS} workers")

    deadline  = float("inf") if LOOP_DURATION == 0 else time.monotonic() + LOOP_DURATION
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
        sleep_for = (max(0.0, POLL_INTERVAL - (time.monotonic() - t0)) if deadline == float("inf")
                     else max(0.0, min(POLL_INTERVAL - (time.monotonic() - t0), remaining)))
        if sleep_for > 0:
            print(f"[poll] sleeping {sleep_for:.0f}s …")
            time.sleep(sleep_for)

    print(f"\n[done] {total_new} mention(s) alerted this run.")


if __name__ == "__main__":
    main()
