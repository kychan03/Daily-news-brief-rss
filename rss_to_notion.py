import os, re, datetime, json
import requests
import feedparser
from dateutil import parser as dateparser
from google import genai

# ==============
# ENV
# ==============
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NEWS_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]          # News Inbox DB id
BRIEFS_DATABASE_ID = os.environ["NOTION_BRIEFS_DATABASE_ID"] # Daily Briefs DB id
BRIEF_MODE = os.environ.get("BRIEF_MODE", "update").strip().lower()  # update | finalize

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

# ==============
# CONST
# ==============
NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

KST = datetime.timezone(datetime.timedelta(hours=9))

# ==============
# HELPERS
# ==============
def normalize_id(s: str) -> str:
    # keep hex only (strip hyphens etc.)
    return re.sub(r"[^0-9a-fA-F]", "", s)

def today_kst_date_str() -> str:
    return datetime.datetime.now(KST).date().isoformat()

def iso_today_start_kst() -> str:
    now = datetime.datetime.now(KST)
    start = datetime.datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=KST)
    return start.isoformat()

def strip_html(s: str) -> str:
    s = re.sub(r"<[^<]+?>", "", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ==============
# NOTION API WRAPPERS
# ==============
def notion_post(path: str, payload: dict):
    r = requests.post(f"{NOTION_API}{path}", headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def notion_patch(path: str, payload: dict):
    r = requests.patch(f"{NOTION_API}{path}", headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def notion_put_block_children(block_id: str, children: list):
    # Notion API: append block children
    payload = {"children": children}
    return notion_patch(f"/blocks/{block_id}/children", payload)

def notion_replace_page_content(page_id: str, markdown_text: str):
    """
    가장 단순한 방식:
    - 페이지의 children을 'append'만 할 수 있어서, 완전 교체는 '기존 블록 삭제'가 필요함.
    - 대신 여기선 실용적으로: 페이지 상단에 '브리프 본문' 섹션을 고정 블록 1개로 관리하는 방식 추천.
    - 구현 난이도를 낮추기 위해, 우선은 '새 내용 추가'로 시작하고, 다음 단계에서 블록 삭제를 붙이자.
    """
    children = [{
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "text": {"content": markdown_text[:2000]}}]}
    }]
    return notion_put_block_children(page_id, children)


def notion_query_db(db_id: str, payload: dict):
    return notion_post(f"/databases/{db_id}/query", payload)

def notion_create_page(parent_db_id: str, properties: dict):
    payload = {"parent": {"database_id": parent_db_id}, "properties": properties}
    return notion_post("/pages", payload)

def notion_update_page(page_id: str, properties: dict):
    payload = {"properties": properties}
    return notion_patch(f"/pages/{page_id}", payload)

# ==============
# RSS INGEST
# ==============
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
            return strip_html(val)
    return None

def news_exists_by_url(url: str) -> bool:
    payload = {"filter": {"property": "URL", "url": {"equals": url}}}
    res = notion_query_db(NEWS_DATABASE_ID, payload)
    return len(res.get("results", [])) > 0

def ingest_feed(feed_url: str, source: str, language: str, region: str, category: str, limit: int = 40):
    feed = feedparser.parse(feed_url)
    if getattr(feed, "bozo", False):
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

# ==============
# FETCH CANDIDATES (KR7 + US3)
# ==============
def fetch_today_candidates(region: str, limit: int):
    start_iso = iso_today_start_kst()

    payload = {
        "page_size": min(100, limit * 10),
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
    t = n["properties"]["Name"]["title"]
    return t[0]["plain_text"] if t else "(no title)"

def page_url_from_news(n):
    return (n["properties"]["URL"].get("url") or "").strip()

def page_summary_from_news(n):
    rt = n["properties"]["Summary"].get("rich_text", [])
    return rt[0]["plain_text"] if rt else ""

def build_top_stories_text(news_items):
    lines = []
    for n in news_items:
        title = page_title_from_news(n)
        url = page_url_from_news(n)
        summ = page_summary_from_news(n)
        if summ:
            lines.append(f"- {title}\n\t- 요약: {summ}\n\t- 링크: {url}")
        else:
            lines.append(f"- {title}\n\t- 링크: {url}")
    return "\n".join(lines)

# ==============
# GEMINI
# ==============
def gemini_generate_keywords_and_stocks(news_items):
    client = genai.Client(api_key=GEMINI_API_KEY)

    items = []
    for n in news_items:
        items.append({
            "title": page_title_from_news(n),
            "summary": page_summary_from_news(n),
            "url": page_url_from_news(n),
        })

    prompt = f"""
너는 뉴스 브리프 편집자이자 시장 분석가야.
아래 뉴스 목록을 기반으로 한국어로만 답해.

[뉴스 목록(JSON)]
{json.dumps(items, ensure_ascii=False)}

출력은 반드시 JSON "하나"만. 마크다운/설명 금지.

반드시 아래 스키마를 정확히 지켜:
{{
  "keywords": [
    "keyword": "...", "one_line": "..."
  ],
  "stock_ideas": [
    {{
      "direction": "bullish|bearish",
      "keyword": "...",
      "thesis": "...",
      "korea": ["ticker":"...", "name":"...", "why":"..."],
      "us": ["ticker":"...", "name":"...", "why":"..."]
    }}
  ]
}}

규칙:
- keywords는 정확히 10개.
- 티커는 확신 없으면 빈 배열([])로.
- 투자 조언이 아니라 아이디어 톤.
"""

    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
    )
    text = resp.text.strip()

    # 가끔 코드블록으로 감싸져 올 수 있어 제거
    text = re.sub(r"^```json\s*", "", text)
    text = re.sub(r"\s*```$", "", text).strip()

    return json.loads(text)

# ==============
# BRIEF UPSERT
# ==============
def upsert_today_brief(news_kr, news_us, finalize: bool):
    date_str = today_kst_date_str()
    title = f"Daily Brief — {date_str}"

    combined = news_kr + news_us
    source_count = len(combined)

    top_stories_text = build_top_stories_text(combined)

    llm = gemini_generate_keywords_and_stocks(combined)

    keywords_lines = [f'- {k["keyword"]}: {k["one_line"]}' for k in llm["keywords"]]
    keywords_text = "\n".join(keywords_lines)

    ideas_lines = []
    for idea in llm.get("stock_ideas", []):
        ideas_lines.append(f'- [{idea["direction"]}] {idea["keyword"]}: {idea["thesis"]}')
        for s in idea.get("korea", []):
            ideas_lines.append(f'  - KR {s.get("ticker","")} {s.get("name","")}: {s.get("why","")}')
        for s in idea.get("us", []):
            ideas_lines.append(f'  - US {s.get("ticker","")} {s.get("name","")}: {s.get("why","")}')
    stock_ideas_text = "\n".join(ideas_lines).strip()

    # Find existing brief page by Title equals
    find_payload = {"filter": {"property": "Title", "title": {"equals": title}}}
    res = notion_query_db(BRIEFS_DATABASE_ID, find_payload)
    existing = res.get("results", [None])[0]

    props = {
        "Title": {"title": [{"text": {"content": title}}]},
        "Date": {"date": {"start": date_str}},
        "Status": {"select": {"name": "Final" if finalize else "Draft"}},
        "Source count": {"number": float(source_count)},
        "Keywords": {"rich_text": [{"text": {"content": keywords_text[:2000]}}]},
        "Top stories": {"rich_text": [{"text": {"content": top_stories_text[:2000]}}]},
        "Stock ideas": {"rich_text": [{"text": {"content": stock_ideas_text[:2000]}}]},
    }

    if existing:
        page_id = existing["id"]
        notion_update_page(page_id, props)
        print(f"[OK] Brief updated: {title} ({'Final' if finalize else 'Draft'})")
    else:
        created = notion_create_page(BRIEFS_DATABASE_ID, props)
        page_id = created["id"]
        print(f"[OK] Brief created: {title} ({'Final' if finalize else 'Draft'})")

    return page_id  # 혹시 나중에 본문 업데이트까지 하고 싶으면 쓰기 좋음

def mark_used_in_brief(news_items):
    for n in news_items:
        notion_update_page(n["id"], {"Used in brief": {"checkbox": True}})
    print(f"[OK] Marked used in brief: {len(news_items)}")

# ==============
# MAIN
# ==============
if __name__ == "__main__":
    NEWS_DATABASE_ID = normalize_id(NEWS_DATABASE_ID)
    BRIEFS_DATABASE_ID = normalize_id(BRIEFS_DATABASE_ID)

    # KR feeds only (US feeds는 다음 단계에서 추가)
    FEEDS = [
        ("https://rss.donga.com/total.xml", "Other", "KO", "KR", "Top"),
        ("https://www.mk.co.kr/rss/30000001/", "Other", "KO", "KR", "Top"),
        ("https://www.khan.co.kr/rss/rssdata/total_news.xml", "Other", "KO", "KR", "Top"),
        # US (Google News RSS)
        ("https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en", "Google News", "EN", "US", "Top"),
        # US Business
        ("https://news.google.com/rss/topics/CAAqJQgKIh9DQkFTRVFvSUwyMHZNRGx1YlY4U0JXVnVMVWRDR2dKVlN5Z0FQAQ?hl=en-US&gl=US&ceid=US:en", "Google News", "EN", "US", "Business"),
    ]
    for feed_url, source, language, region, category in FEEDS:
        ingest_feed(feed_url, source, language, region, category, limit=40)

    news_kr = fetch_today_candidates("KR", 7)
    news_us = fetch_today_candidates("US", 3)  # 아직 US 피드 없으면 0개일 수 있음

    finalize = (BRIEF_MODE == "finalize")
    _page_id = upsert_today_brief(news_kr, news_us, finalize=finalize)

    if finalize:
        mark_used_in_brief(news_kr + news_us)
