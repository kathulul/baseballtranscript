"""
Scrape ASAP Sports baseball interview transcripts.
Flow: landing (letters A–Z) → letter page (players) → player page (interviews) → interview page (transcript).
Output: single CSV with resume support and rate limiting.
"""

import csv
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup


# Master link: baseball category (id=2). Base URL and category derived from it.
LANDING_URL = "https://www.asapsports.com/showcat.php?id=2"
_landing = urlparse(LANDING_URL)
BASE_URL = f"{_landing.scheme}://{_landing.netloc}"
CATEGORY_ID = parse_qs(_landing.query).get("id", ["2"])[0]

RATE_LIMIT_SEC = 0.8
CSV_PATH = Path(__file__).resolve().parent.parent / "asap_baseball_transcripts.csv"
CSV_COLUMNS = [
    "player_name",
    "interview_title",
    "date",
    "event",
    "venue",
    "team",
    "session_type",
    "interview_id",
    "url",
    "transcript",
]


def load_scraped_ids(csv_path: Path) -> set[str]:
    """Load existing interview IDs from CSV for resume support."""
    if not csv_path.exists():
        return set()
    ids = set()
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        try:
            reader = csv.DictReader(f)
            if reader.fieldnames and "interview_id" in reader.fieldnames:
                for row in reader:
                    if row.get("interview_id"):
                        ids.add(row["interview_id"].strip())
        except (csv.Error, UnicodeDecodeError):
            pass
    return ids


def get_soup(session: requests.Session, url: str) -> BeautifulSoup | None:
    """Fetch URL and return BeautifulSoup; None on failure."""
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except requests.RequestException:
        return None


def get_letter_urls() -> list[str]:
    """Return list of letter index URLs (one per letter a–z)."""
    return [
        f"{BASE_URL}/show_player.php?category={CATEGORY_ID}&letter={letter}"
        for letter in "abcdefghijklmnopqrstuvwxyz"
    ]


def get_player_links(soup: BeautifulSoup) -> list[tuple[str, str]]:
    """From a letter page soup, return [(player_url, player_name), ...]."""
    if not soup:
        return []
    seen = set()
    out = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "show_player.php?id=" in href and "letter=" not in href:
            url = urljoin(BASE_URL, href)
            name = (a.get_text() or "").strip()
            if name and url not in seen:
                seen.add(url)
                out.append((url, name))
    return out


def get_interview_links(soup: BeautifulSoup) -> list[tuple[str, str, str]]:
    """From a player page soup, return [(interview_url, interview_title, date), ...]. Date from <nobr>[ ... ]</nobr> in same row."""
    if not soup:
        return []
    out = []
    for a in soup.find_all("a", href=True):
        if "show_interview.php?id=" not in a.get("href", ""):
            continue
        url = urljoin(BASE_URL, a["href"])
        title = (a.get_text() or "").strip()
        if not title:
            continue
        date = ""
        tr = a.find_parent("tr")
        if tr:
            nobr = tr.find("nobr")
            if nobr:
                date = re.sub(r"^\[|\]$", "", (nobr.get_text() or "").strip())
        out.append((url, title, date))
    return out


def parse_interview_id(url: str) -> str | None:
    """Extract id from show_interview.php?id=..."""
    qs = parse_qs(urlparse(url).query)
    ids = qs.get("id", [])
    return ids[0] if ids else None


def extract_transcript_metadata_and_text(soup: BeautifulSoup) -> dict:
    """Extract event, date, player, venue, team, session_type and full transcript."""
    data = {
        "player_name": "",
        "interview_title": "",
        "date": "",
        "event": "",
        "venue": "",
        "team": "",
        "session_type": "",
        "transcript": "",
    }
    if not soup:
        return data

    # Title often in h1
    h1 = soup.find("h1")
    if h1:
        data["event"] = (h1.get_text() or "").strip()
        data["interview_title"] = data["event"]

    # Date is in h2 on ASAP pages (e.g. "October 4, 2023")
    h2 = soup.find("h2")
    if h2:
        t = (h2.get_text() or "").strip()
        if re.match(r"^[A-Z][a-z]+\s+\d{1,2},?\s+\d{4}$", t):
            data["date"] = t

    # h3 blocks: first = interviewee name, then venue, team, session type
    all_h3 = soup.find_all("h3")
    texts = [(h.get_text() or "").strip() for h in all_h3]
    for i, t in enumerate(texts):
        if not t:
            continue
        if re.match(r"^[A-Z][a-z]+\s+\d{1,2},?\s+\d{4}$", t):
            data["date"] = data["date"] or t
        elif not data["player_name"]:
            data["player_name"] = t
        elif any(x in t.lower() for x in ("field", "stadium", "park", "center")):
            data["venue"] = t
        elif any(x in t.lower() for x in ("press", "conference", "session")):
            data["session_type"] = t
        elif not data["team"] and any(x in t.lower() for x in (
            "twins", "yankees", "sox", "mets", "dodgers", "braves", "guardians",
            "angels", "mariners", "athletics", "rangers", "royals", "tigers",
            "rays", "orioles", "blue jays", "nationals", "phillies", "marlins",
            "cardinals", "cubs", "brewers", "pirates", "reds", "astros",
            "giants", "padres", "diamondbacks", "rockies", "indians"
        )):
            data["team"] = t
        elif not data["venue"] and "," in t and i >= 3 and any(
            x in t.lower() for x in ("field", "stadium", "park", "center", "arena")
        ):
            data["venue"] = t

    # Transcript lives in the same <td> as the main content (h1). Nav/sidebar are elsewhere.
    main_td = soup.find("h1") and soup.find("h1").find_parent("td")
    transcript_parts = []
    if main_td:
        for p in main_td.find_all("p"):
            text = (p.get_text() or "").strip()
            if "FastScripts Transcript" in text or "ASAP Sports, Inc" in text:
                break
            if text:
                transcript_parts.append(text)
    if not transcript_parts and main_td:
        text = main_td.get_text(separator="\n", strip=True)
        for marker in ["FastScripts Transcript", "ASAP Sports, Inc."]:
            if marker in text:
                text = text.split(marker)[0]
        transcript_parts = [text.strip()] if text.strip() else []
    data["transcript"] = "\n\n".join(transcript_parts) if transcript_parts else _fallback_transcript_text(soup)
    return data


def _fallback_transcript_text(soup: BeautifulSoup) -> str:
    """Fallback: main content area text, excluding nav/footer."""
    for tag in soup.find_all(["script", "style", "nav"]):
        tag.decompose()
    main = soup.find(id=re.compile(r"content|main|transcript", re.I)) or soup.find("main")
    body = main if main else soup.find("body") or soup
    if not body:
        return ""
    text = body.get_text(separator="\n", strip=True)
    for marker in ["FastScripts Transcript", "ASAP Sports, Inc.", "Subscribe to RSS"]:
        if marker in text:
            text = text.split(marker)[0]
    return text.strip()


def scrape_interview(
    session: requests.Session,
    url: str,
    interview_id: str,
    player_name: str,
    interview_title: str,
    date_from_player_page: str = "",
) -> dict | None:
    """Fetch one interview page and return a row dict for CSV. Name/date from player page override when provided."""
    soup = get_soup(session, url)
    time.sleep(RATE_LIMIT_SEC)
    if not soup:
        return None
    row = extract_transcript_metadata_and_text(soup)
    row["player_name"] = player_name
    row["interview_title"] = row["interview_title"] or interview_title
    row["date"] = date_from_player_page or row["date"]
    row["interview_id"] = interview_id
    row["url"] = url
    return row


def ensure_csv_header(csv_path: Path) -> None:
    """Create CSV with header if file doesn't exist or is empty."""
    if csv_path.exists() and csv_path.stat().st_size > 0:
        return
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()


def append_row(csv_path: Path, row: dict) -> None:
    """Append one row to CSV."""
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow(row)


def run(
    csv_path: Path = CSV_PATH,
    rate_limit_sec: float = RATE_LIMIT_SEC,
    resume: bool = True,
    max_letters: int | None = None,
) -> None:
    """Full scrape: letters → players → interviews → transcripts, write to CSV.
    Set max_letters (e.g. 1) to limit scope for testing; None = all A–Z.
    """
    global RATE_LIMIT_SEC
    RATE_LIMIT_SEC = rate_limit_sec
    scraped_ids = load_scraped_ids(csv_path) if resume else set()
    ensure_csv_header(csv_path)
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; ASAP transcript scraper)"})

    letter_urls = get_letter_urls()
    if max_letters is not None:
        letter_urls = letter_urls[:max_letters]
    total_new = 0
    for letter_url in letter_urls:
        soup = get_soup(session, letter_url)
        time.sleep(RATE_LIMIT_SEC)
        players = get_player_links(soup)
        for player_url, player_name_from_letter in players:
            psoup = get_soup(session, player_url)
            time.sleep(RATE_LIMIT_SEC)
            player_name = (psoup.find("h1").get_text() or "").strip() if psoup and psoup.find("h1") else player_name_from_letter
            interviews = get_interview_links(psoup)
            for interview_url, interview_title, date_on_player_page in interviews:
                iid = parse_interview_id(interview_url)
                if not iid:
                    continue
                if iid in scraped_ids:
                    continue
                row = scrape_interview(
                    session, interview_url, iid, player_name, interview_title, date_on_player_page
                )
                if row:
                    append_row(csv_path, row)
                    scraped_ids.add(iid)
                    total_new += 1
    print(f"Done. New transcripts written: {total_new}. CSV: {csv_path}")


if __name__ == "__main__":
    run()
