import asyncio
import csv
import os
import random
import re
from datetime import datetime
from playwright.async_api import async_playwright

BASE_URL = "https://www.hotnigerianjobs.com/jobs/featured"

# Max pages to scrape per run
# Each page has ~20 listings
MAX_PAGES = 20

OUTPUT_FILE = "data/raw/hotnigerianjobs_raw.csv"
FIELDS = ["title", "company", "date_posted", "url", "scraped_at"]

# Suffixes to strip from the combined title string to extract company name
TITLE_SUFFIXES = [
    " Job Recruitment",
    " Recruitment",
    " Job Vacancies",
    " Vacancies",
    " Latest Jobs",
    " Jobs",
    " Hiring",
    " is Recruiting",
    " Career Opportunities",
]


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

def clean(text):
    """Remove extra whitespace and newlines."""
    return " ".join(text.split()) if text else ""


def extract_company(raw_title):
    """Strip job-related suffixes to isolate the company name.
    e.g. 'Oando Plc Job Recruitment' -> 'Oando Plc'
    """
    title = raw_title
    for suffix in TITLE_SUFFIXES:
        if title.lower().endswith(suffix.lower()):
            title = title[: -len(suffix)].strip()
            break
    return title


def extract_date(raw):
    """Pull just the date from strings like 'Posted on Mon 20th Apr, 2026 - '"""
    match = re.search(r"\d{1,2}\w{2}\s+\w+,?\s+\d{4}", raw)
    return match.group(0).strip() if match else clean(raw)


def load_existing_urls():
    """Read URLs already saved in the CSV so we can skip them."""
    if not os.path.exists(OUTPUT_FILE):
        return set()
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["url"] for row in reader}


def save_to_csv(jobs):
    """Append new jobs to CSV. Writes header only on first ever run."""
    os.makedirs("data/raw", exist_ok=True)
    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(jobs)
    print(f"\nDone. Appended {len(jobs)} new jobs to {OUTPUT_FILE}")


def deduplicate(jobs):
    """Remove duplicates within the current batch."""
    seen = set()
    unique = []
    for job in jobs:
        key = (job["title"].lower(), job["company"].lower())
        if key not in seen:
            seen.add(key)
            unique.append(job)
    removed = len(jobs) - len(unique)
    if removed:
        print(f"Removed {removed} duplicate listings from this batch")
    return unique


# ─────────────────────────────────────────
# Scraping
# ─────────────────────────────────────────

async def scrape_page(page, url):
    """Scrape all job cards from a single listing page.
    Returns (jobs, has_next_page).
    """
    jobs = []
    has_next = False

    try:
        await page.goto(url, timeout=30000)
        await page.wait_for_selector("div.mycase", timeout=15000)

        cards = await page.query_selector_all("div.mycase")

        for card in cards:
            try:
                # Title link — inside h1 > a
                link_el = await card.query_selector("div.jobheader h1 a")
                raw_title = await link_el.inner_text() if link_el else ""
                href = await link_el.get_attribute("href") if link_el else ""

                # Split into job title and company
                company = extract_company(clean(raw_title))
                # Title is the full heading — company name is the meaningful part
                title = clean(raw_title)

                # Date — first span.semibio contains the posted date
                date_el = await card.query_selector("span.semibio")
                raw_date = await date_el.inner_text() if date_el else ""
                date_posted = extract_date(raw_date)

                if title:
                    jobs.append({
                        "title":       title,
                        "company":     company,
                        "date_posted": date_posted,
                        "url":         href,
                        "scraped_at":  datetime.now().isoformat(),
                    })

            except Exception as e:
                print(f"    [!] Error parsing card: {e}")
                continue

        # Check if a next page link exists
        next_btn = await page.query_selector("a[rel='next'], a[aria-label='Next']")
        has_next = next_btn is not None

    except Exception as e:
        print(f"  [!] Failed to load {url}: {e}")

    return jobs, has_next


async def scrape_all(existing_urls):
    """Scrape all pages up to MAX_PAGES, skipping already collected listings."""
    all_jobs = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        for page_num in range(1, MAX_PAGES + 1):
            url = f"{BASE_URL}/{page_num}/"
            print(f"\n[Page {page_num}/{MAX_PAGES}] {url}")

            jobs, has_next = await scrape_page(page, url)

            # Filter out already collected listings
            new_jobs = [j for j in jobs if j["url"] not in existing_urls]
            all_jobs.extend(new_jobs)
            print(f"  {len(jobs)} listings found — {len(new_jobs)} new")

            # Stop early if whole page already collected
            if len(new_jobs) == 0:
                print(f"  All listings already collected — stopping early")
                break

            if not has_next:
                print(f"  No more pages after page {page_num}")
                break

            # Polite delay between pages
            delay = random.uniform(2, 4)
            print(f"  Waiting {delay:.1f}s...")
            await asyncio.sleep(delay)

        await browser.close()

    return all_jobs


# ─────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  HotNigerianJobs scraper")
    print("=" * 50)
    print(f"Max pages : {MAX_PAGES}")
    print(f"Max jobs  : ~{MAX_PAGES * 20:,}")
    print()

    # Load existing URLs to skip already collected listings
    existing_urls = load_existing_urls()
    print(f"Existing listings in CSV : {len(existing_urls)}")
    print(f"These will be skipped.\n")

    # Scrape
    jobs = asyncio.run(scrape_all(existing_urls))

    # Clean and save
    jobs = deduplicate(jobs)
    save_to_csv(jobs)

    print(f"\nTotal new listings added this run : {len(jobs)}")
    print(f"Run again any time to collect new postings.")