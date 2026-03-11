import os
import re
import datetime
import json
import requests
import feedparser
from dateutil import parser as dateparser
from google import genai

# ========= ENV (환경 변수) =========
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NEWS_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")
BRIEFS_DATABASE_ID = os.environ.get("NOTION_BRIEFS_DATABASE_ID")
HUB_PAGE_ID = os.environ.get("NOTION_HUB_PAGE_ID")
BRIEF_MODE = os.environ.get("BRIEF_MODE", "update").strip().lower()

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

# ========= CONST (상수) =========
# 아래 URL에 대괄호([])나 소괄호(())가 절대 포함되지 않아야 합니다.
NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

KST = datetime.timezone(datetime.timedelta(hours=9))

# ========= HELPERS (유틸리티) =========
def normalize_id(s: str) -> str:
    if not s: return ""
    # 노션 ID에서 하이픈 제거 및 순수 16진수만 추출
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

# ========= NOTION API 호출 함수 =========
def notion_get(path: str):
    url = f"{NOTION_API}{path}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def notion_post(path: str, payload: dict):
    url = f"{NOTION_API}{path}"
    r = requests.post(url, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def notion_patch(path: str, payload: dict):
    url = f"{NOTION_API}{path}"
    r = requests.patch(url, headers=HEADERS, json=payload, timeout=30)
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

def notion_list_block_children(block_id: str, page_size: int = 100):
    return notion_get(f"/blocks/{block_id}/children?page_size={page_size}")

def notion_append_block_children(block_id: str, children: list):
    payload = {"children": children}
    return notion_patch(f"/blocks/{block_id}/children", payload)

def notion_delete_block(block_id: str):
    return notion_patch(f"/blocks/{block_id}", {"archived": True})

def find_toggle_block_id_by_title(page_id: str, toggle_title: str) -> str | None:
    res = notion_list_block_children(page_id)
    for b in res.get("results", []):
        if b.get("type") != "toggle":
            continue
        rt = b["toggle"].get("rich_text", [])
        title = "".join([t.get("plain_text", "") for t in rt]).strip()
        if title == toggle_title:
            return b["id"]
    return None

def to_paragraph(text: str):
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "text": {"content": (text or "")[:2000]}}]},
    }

def replace_toggle_children(toggle_block_id: str, new_blocks: list):
    res = notion_list_block_children(toggle_block_id)
    old_children = res.get("results", [])
    for ch in old_children:
        notion_delete_block(ch["id"])
    if new_blocks:
        for i in range(0, len(new_blocks), 100):
            notion_append_block_children(toggle_block_id, new_blocks[i:i+100])

# ========= RSS 데이터 처리 =========
def parse_published(entry) -> str | None:
    for key in ["published", "updated"]:
        if entry.get(key):
            try:
                return dateparser.parse(entry[key]).isoformat()
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
    count_new = 0
    for entry in feed.entries[:limit]:
        title = (entry.get("title") or "").strip()
        link = (entry.get("link") or "").strip()
        if not title or not link: continue
        if news_exists_by_url(link): continue

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

# ========= 후보 뉴스 추출 =========
def fetch_today_candidates(region: str, limit: int):
    start_iso = iso_today_start_kst()
    payload = {
        "page_size": 100,
        "filter": {
            "and": [
                {"or": [
                    {"property": "Published", "date": {"on_or_after": start_iso}},
                    {"property": "Created", "created_time": {"on_or_after": start_iso}},
                ]},
                {"property": "Used in brief", "checkbox": {"equals": False}},
                {"property": "Region", "select": {"equals": region}},
            ]
        },
        "sorts": [
            {"property": "Published", "direction": "descending"},
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
        lines.append(f"- {title}\n  링크: {url}")
    return "\n".join(lines)

# ========= Gemini AI 연동 =========
def gemini_generate_keywords_and_stocks(news_items):
    client = genai.Client(api_key=GEMINI_API_KEY)
    items = [{"title": page_title_from_news(n), "summary": page_summary_from_news(n)} for n in news_items]

    prompt = f"""
너는 전문 뉴스 에디터이자 금융 분석가야. 아래 뉴스 목록을 분석해서 한국어로 응답해.
반드시 순수 JSON 데이터만 출력하고, 마크다운 코드 블록(```json)이나 다른 설명은 절대 포함하지 마.

[뉴스 데이터]
{json.dumps(items, ensure_ascii=False)}

스키마 형식:
{{
  "keywords": [
    {{"keyword": "키워드", "one_line": "한줄 요약"}}
  ],
  "stock_ideas": [
    {{
      "direction": "bullish|bearish",
      "keyword": "관련 테마",
      "thesis": "분석 이유",
      "korea": [{{"ticker":"종목코드","name":"종목명","why":"선정이유"}}],
      "us": [{{"ticker":"티커","name":"종목명","why":"선정이유"}}]
    }}
  ]
}}
"""
    resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
    text = resp.text.strip()
    # 혹시 모를 마크다운 기호 제거
    text = text.replace("```json", "").replace("```", "").strip()
    return json.loads(text)

# ========= 브리프 생성 및 업데이트 =========
def upsert_today_brief(news_kr, news_us, finalize: bool):
    date_str = today_kst_date_str()
    title = f"Daily Brief — {date_str}"
    combined = news_kr + news_us
    
    top_stories_text = build_top_stories_text(combined)
    llm = gemini_generate_keywords_and_stocks(combined)

    keywords_lines = [f'- {k["keyword"]}: {k["one_line"]}' for k in llm.get("keywords", [])]
    keywords_text = "\n".join(keywords_lines)

    ideas_lines = []
    for idea in llm.get("stock_ideas", []):
        ideas_lines.append(f'- [{idea["direction"]}] {idea["keyword"]}: {idea["thesis"]}')
        for s in idea.get("korea", []):
            ideas_lines.append(f'  · KR {s.get("ticker","")} {s.get("name","")}: {s.get("why","")}')
        for s in idea.get("us", []):
            ideas_lines.append(f'  · US {s.get("ticker","")} {s.get("name","")}: {s.get("why","")}')
    stock_ideas_text = "\n".join(ideas_lines).strip()

    find_payload = {"filter": {"property": "Title", "title": {"equals": title}}}
    res = notion_query_db(BRIEFS_DATABASE_ID, find_payload)
    existing = res.get("results", [None])[0]

    props = {
        "Title": {"title": [{"text": {"content": title}}]},
        "Date": {"date": {"start": date_str}},
        "Status": {"select": {"name": "Final" if finalize else "Draft"}},
        "Source count": {"number": float(len(combined))},
        "Keywords": {"rich_text": [{"text": {"content": keywords_text[:2000]}}]},
        "Top stories": {"rich_text": [{"text": {"content": top_stories_text[:2000]}}]},
        "Stock ideas": {"rich_text": [{"text": {"content": stock_ideas_text[:2000]}}]},
    }

    if existing:
        page_id = existing["id"]
        notion_update_page(page_id, props)
    else:
        created = notion_create_page(BRIEFS_DATABASE_ID, props)
        page_id = created["id"]

    toggle_id = find_toggle_block_id_by_title(HUB_PAGE_ID, "AUTO_BRIEF")
    if toggle_id:
        blocks = [
            to_paragraph(f"📌 {title} ({'Final' if finalize else 'Draft'})"),
            to_paragraph(f"마지막 업데이트: {datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M')} KST"),
            to_paragraph(""),
            to_paragraph("✅ 오늘의 키워드"),
            to_paragraph(keywords_text),
            to_paragraph(""),
            to_paragraph("📈 시장 아이디어"),
            to_paragraph(stock_ideas_text),
        ]
        replace_toggle_children(toggle_id, blocks)
    
    return page_id

def mark_used_in_brief(news_items):
    for n in news_items:
        notion_update_page(n["id"], {"Used in brief": {"checkbox": True}})

# ========= 실행 메인 루틴 =========
if __name__ == "__main__":
    # ID 정규화 (하이픈 등 제거)
    NEWS_DATABASE_ID = normalize_id(NEWS_DATABASE_ID)
    BRIEFS_DATABASE_ID = normalize_id(BRIEFS_DATABASE_ID)
    HUB_PAGE_ID = normalize_id(HUB_PAGE_ID)

    FEEDS = [
        ("[https://rss.donga.com/total.xml](https://rss.donga.com/total.xml)", "Donga", "KO", "KR", "Top"),
        ("[https://www.mk.co.kr/rss/30000001/](https://www.mk.co.kr/rss/30000001/)", "MK", "KO", "KR", "Top"),
        ("[https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en](https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en)", "Google News", "EN", "US", "Top"),
    ]

    for f_url, src, lang, reg, cat in FEEDS:
        try:
            ingest_feed(f_url, src, lang, reg, cat)
        except Exception as e:
            print(f"[ERR] Feed failed: {src} - {e}")

    news_kr = fetch_today_candidates("KR", 7)
    news_us = fetch_today_candidates("US", 3)

    if news_kr or news_us:
        is_final = (BRIEF_MODE == "finalize")
        upsert_today_brief(news_kr, news_us, finalize=is_final)
        if is_final:
            mark_used_in_brief(news_kr + news_us)
        print(f"[SUCCESS] {datetime.datetime.now(KST)} - 브리프 작업 완료")
    else:
        print("[SKIP] 새로운 뉴스 데이터가 없습니다.")
        
