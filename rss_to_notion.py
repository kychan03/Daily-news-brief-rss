import os, re
import requests
import feedparser
from dateutil import parser as dateparser

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

def normalize_id(s: str) -> str:
    return re.sub(r"[^0-9a-fA-F]", "", s)

def notion_query_by_url(article_url: str) -> bool:
    payload = {
        "filter": {
            "property": "URL",
            "url": {"equals": article_url}
        }
    }
    r = requests.post(f"{NOTION_API}/databases/{DATABASE_ID}/query", headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return len(r.json().get("results", [])) > 0

def notion_create_page(title: str, url: str, published_iso: str | None,
                       source: str, language: str, region: str, category: str,
                       summary: str | None):
    props = {
        "Name": {"title": [{"text": {"content": title[:200]}}]},
        "URL": {"url": url},
        "Source": {"select": {"name": source}},
        "Language": {"select": {"name": language}},
        "Region": {"select": {"name": region}},
        "Category": {"select": {"name": category}},
    }
    if published_iso:
        props["Published"] = {"date": {"start": published_iso}}
    if summary:
        props["Summary"] = {"rich_text": [{"text": {"content": summary[:2000]}}]}

    payload = {"parent": {"database_id": DATABASE_ID}, "properties": props}
    r = requests.post(f"{NOTION_API}/pages", headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()

def parse_published(entry) -> str | None:
    for key in ["published", "updated"]:
        if entry.get(key):
            try:
                return dateparser.parse(entry[key]).isoformat()
            except Exception:
                pass
    if getattr(entry, "published_parsed", None):
        try:
            import datetime
            dt = datetime.datetime(*entry.published_parsed[:6])
            return dt.isoformat()
        except Exception:
            pass
    return None

def pick_summary(entry) -> str | None:
    for key in ["summary", "description"]:
        val = entry.get(key)
        if val:
            txt = re.sub("<[^<]+?>", "", val)
            return re.sub(r"\s+", " ", txt).strip()
    return None

def ingest_feed(feed_url: str, source: str, language: str, region: str, category: str, limit: int = 40):
    feed = feedparser.parse(feed_url)
    if feed.bozo:
        print(f"[WARN] feed parse issue: {feed_url} err={feed.bozo_exception}")

    count_new = 0
    for entry in feed.entries[:limit]:
        title = (entry.get("title") or "").strip()
        link = (entry.get("link") or "").strip()
        if not title or not link:
            continue

        # de-dup by URL property
        if notion_query_by_url(link):
            continue

        published = parse_published(entry)
        summary = pick_summary(entry)

        notion_create_page(
            title=title,
            url=link,
            published_iso=published,
            source=source,
            language=language,
            region=region,
            category=category,
            summary=summary
        )
        count_new += 1

    print(f"[OK] {source} new items: {count_new}")

if __name__ == "__main__":
    # Notion accepts both hyphenated + non-hyphenated, normalize anyway
    DATABASE_ID = normalize_id(DATABASE_ID)

    FEEDS = [
        # KR (일단 확정 3개)
        ("https://rss.donga.com/total.xml", "Other", "KO", "KR", "Top"),
        ("https://www.mk.co.kr/rss/30000001/", "Other", "KO", "KR", "Top"),
        ("https://www.khan.co.kr/rss/rssdata/total_news.xml", "Other", "KO", "KR", "Top"),
    ]

    for feed_url, source, language, region, category in FEEDS:
        ingest_feed(feed_url, source, language, region, category, limit=40)
