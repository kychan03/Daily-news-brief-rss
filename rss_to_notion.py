import os, re, datetime
import requests
import feedparser
from dateutil import parser as dateparser

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NEWS_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]  # News Inbox
BRIEFS_DATABASE_ID = os.environ["NOTION_BRIEFS_DATABASE_ID"]  # Daily Briefs
BRIEF_MODE = os.environ.get("BRIEF_MODE", "update").strip().lower()  # update | finalize

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

KST = datetime.timezone(datetime.timedelta(hours=9))

def normalize_id(s: str) -> str:
    return re.sub(r"[^0-9a-fA-F]", "", s)

def today_kst_date_str() -> str:
    return datetime.datetime.now(KST).date().isoformat()

def iso_today_range_kst():
    # [today 00:00, tomorrow 00:00) in KST, expressed in ISO with offset
    now = datetime.datetime.now(KST)
    start = datetime.datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=KST)
    end = start + datetime.timedelta(days=1)
    return start.isoformat(), end.isoformat()

def notion_post(path, payload):
    r = requests.post(f"{NOTION_API}{path}", headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def notion_patch(path, payload):
    r = requests.patch(f"{NOTION_API}{path}", headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def notion_query_db(db_id: str, payload: dict):
    return notion_post(f"/databases/{db_id}/query", payload)

def notion_create_page(parent_db_id: str, properties: dict):
    payload = {"parent": {"database_id": parent_db_id}, "properties": properties}
    return notion_post("/pages", payload)

def notion_update_page(page_id: str, properties: dict):
    payload = {"properties": properties}
    return notion_patch(f"/pages/{page_id}", payload)

def parse_published(entry) -> str | None:
    for key in ["published", "updated"]:
        if entry.get(key):
            try:
                return dateparser.parse(entry[key]).isoformat()
            except Exception:
                pass
    if getattr(entry, "published_parsed", None):
        try:
            dt = datetime.datetime(*entry.published_parsed[:6], tzinfo=datetime.timezone.utc)
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

def news_exists_by_url(url: str) -> bool:
    payload = {"filter": {"property": "URL", "url": {"equals": url}}}
    res = notion_query_db(NEWS_DATABASE_ID, payload)
    return len(res.get("results", [])) > 0

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

        if news_exists_by_url(link):
            continue

        published = parse_published(entry)
        summary = pick_summary(entry)

        props = {
            "Name": {"title": [{"text": {"content": title[:200]}}]},
            "URL": {"url": link},
            "Source": {"select": {"name": source}},
            "Language": {"select": {"name": language}},
            "Region": {"select": {"name": region}},
            "Category": {"select": {"name": category}},
        }
        if published:
            props["Published"] = {"date": {"start": published}}
        if summary:
            props["Summary"] = {"rich_text": [{"text": {"content": summary[:2000]}}]}

        notion_create_page(NEWS_DATABASE_ID, props)
        count_new += 1

    print(f"[OK] {source} new items: {count_new}")

def fetch_today_candidates(region: str, limit: int):
    start_iso, end_iso = iso_today_range_kst()

    # Filter: (Published within today OR Created within today) AND Used in brief is false AND Region = region
    payload = {
        "page_size": min(100, limit * 5),
        "filter": {
            "and": [
                {
                    "or": [
                        {"property": "Published", "date": {"on_or_after": start_iso}},
                        {"property": "Created", "created_time": {"on_or_after": start_iso}},
                    ]
                },
                {"property": "Used in brief", "checkbox": {"equals": False}},
                {"property": "Region", "select": {"equals": region}},
            ]
        },
        "sorts": [
            {"property": "Published", "direction": "descending"},
            {"property": "Created", "direction": "descending"},
        ],
    }

    res = notion_query_db(NEWS_DATABASE_ID, payload)
    return res.get("results", [])[:limit]

def page_title_from_news(n):
    # Notion API title extraction
    t = n["properties"]["Name"]["title"]
    return t[0]["plain_text"] if t else "(no title)"

def page_url_from_news(n):
    u = n["properties"]["URL"].get("url")
    return u or ""

def page_summary_from_news(n):
    rt = n["properties"]["Summary"].get("rich_text", [])
    return rt[0]["plain_text"] if rt else ""

def build_brief_text(news_items):
    # Simple baseline: use titles + summaries (no LLM)
    # Later we can add keyword extraction & stock mapping rules.
    lines = []
    for i, n in enumerate(news_items, 1):
        title = page_title_from_news(n)
        url = page_url_from_news(n)
        summ = page_summary_from_news(n)
        if summ:
            lines.append(f"- {title}\n  - {summ}\n  - {url}")
        else:
            lines.append(f"- {title}\n  - {url}")
    return "\n".join(lines)

def upsert_today_brief(news_kr, news_us, finalize: bool):
    date_str = today_kst_date_str()
    title = f"Daily Brief — {date_str}"

    combined = news_kr + news_us
    source_count = len(combined)

    # Minimal keyword placeholder (later upgrade)
    keywords = "자동 생성 준비 중 (다음 단계에서 키워드+한줄설명 생성 로직 추가)"

    top_stories = build_brief_text(combined)
    stock_ideas = "자동 생성 준비 중 (다음 단계에서 키워드→KR/US 주식 매핑 규칙 추가)"

    # Find existing brief page by Title equals
    find_payload = {"filter": {"property": "Title", "title": {"equals": title}}}
    res = notion_query_db(BRIEFS_DATABASE_ID, find_payload)
    existing = res.get("results", [None])[0]

    props = {
        "Title": {"title": [{"text": {"content": title}}]},
        "Date": {"date": {"start": date_str}},
        "Keywords": {"rich_text": [{"text": {"content": keywords[:2000]}}]},
        "Top stories": {"rich_text": [{"text": {"content": top_stories[:2000]}}]},
        "Stock ideas": {"rich_text": [{"text": {"content": stock_ideas[:2000]}}]},
        "Source count": {"number": float(source_count)},
        "Status": {"select": {"name": "Final" if finalize else "Draft"}},
    }

    if existing:
        page_id = existing["id"]
        notion_update_page(page_id, props)
        print(f"[OK] Brief updated: {title} ({'Final' if finalize else 'Draft'})")
    else:
        notion_create_page(BRIEFS_DATABASE_ID, props)
        print(f"[OK] Brief created: {title} ({'Final' if finalize else 'Draft'})")

def mark_used_in_brief(news_items):
    for n in news_items:
        page_id = n["id"]
        notion_update_page(page_id, {"Used in brief": {"checkbox": True}})
    print(f"[OK] Marked used in brief: {len(news_items)}")

if __name__ == "__main__":
    NEWS_DATABASE_ID = normalize_id(NEWS_DATABASE_ID)
    BRIEFS_DATABASE_ID = normalize_id(BRIEFS_DATABASE_ID)

    # 1) RSS ingest (그대로 유지)
    FEEDS = [
        ("https://rss.donga.com/total.xml", "Other", "KO", "KR", "Top"),
        ("https://www.mk.co.kr/rss/30000001/", "Other", "KO", "KR", "Top"),
        ("https://www.khan.co.kr/rss/rssdata/total_news.xml", "Other", "KO", "KR", "Top"),
    ]
    for feed_url, source, language, region, category in FEEDS:
        ingest_feed(feed_url, source, language, region, category, limit=40)

    # 2) Build/update brief
    news_kr = fetch_today_candidates("KR", 7)
    news_us = fetch_today_candidates("US", 3)

    finalize = (BRIEF_MODE == "finalize")
    upsert_today_brief(news_kr, news_us, finalize=finalize)

    # 3) If finalize, mark used items
    if finalize:
        mark_used_in_brief(news_kr + news_us)
