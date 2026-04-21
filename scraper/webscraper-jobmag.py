import asyncio
import csv
import os
import random
from datetime import datetime
from urllib.robotparser import RobotFileParser
from playwright.async_api import async_playwright

BASE_URL = "https://www.myjobmag.com/jobs"
ROBOTS_URL = "https://www.myjobmag.com/robots.txt"

# Max pages to scrape per run
# Each page has ~20 listings
MAX_PAGES = 20

OUTPUT_FILE = "data/raw/myjobmag_raw.csv"
FIELDS = ["title", "company", "date_posted", "url", "scraped_at"]


# ─────────────────────────────────────────
# Robots.txt compliance
# ─────────────────────────────────────────

def load_robot_rules():
    """Fetch and parse MyJobMag's robots.txt."""
    rp = RobotFileParser()
    rp.set_url(ROBOTS_URL)
    try:
        rp.read()
        print(f"Loaded robots.txt from {ROBOTS_URL}")
    except Exception as e:
        print(f"[!] Could not read robots.txt: {e}")
        print("    Proceeding with caution — only scraping /jobs/ pages")
    return rp


def is_allowed(rp, url):
    """Return True if robots.txt permits scraping this URL."""
    try:
        return rp.can_fetch("*", url)
    except Exception:
        return "/jobs" in url


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

def clean(text):
    """Remove extra whitespace and newlines."""
    return " ".join(text.split()) if text else ""


def split_title_company(full_title):
    """Split 'Job Title at Company Name' into separate fields."""
    if " at " in full_title:
        parts = full_title.split(" at ", 1)
        return clean(parts[0]), clean(parts[1])
    return clean(full_title), ""


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
        await page.wait_for_selector("li.job-list-li", timeout=15000)

        cards = await page.query_selector_all("li.job-list-li")

        for card in cards:
            try:
                # Title + company combined in the <h2> link
                h2 = await card.query_selector("li.mag-b h2 a")
                full_title = await h2.inner_text() if h2 else ""
                title, company = split_title_company(full_title)

                # Job detail URL
                href = await h2.get_attribute("href") if h2 else ""
                if href and href.startswith("/"):
                    href = f"https://www.myjobmag.com{href}"

                # Date posted
                date_el = await card.query_selector("li#job-date")
                date_posted = await date_el.inner_text() if date_el else ""

                if title:
                    jobs.append({
                        "title":       clean(title),
                        "company":     clean(company),
                        "date_posted": clean(date_posted),
                        "url":         href,
                        "scraped_at":  datetime.now().isoformat(),
                    })

            except Exception as e:
                print(f"    [!] Error parsing card: {e}")
                continue

        # Check if a next page exists
        next_btn = await page.query_selector("a[rel='next'], a[aria-label='Next']")
        has_next = next_btn is not None

    except Exception as e:
        print(f"  [!] Failed to load {url}: {e}")

    return jobs, has_next


async def scrape_all(existing_urls, rp):
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
            url = BASE_URL if page_num == 1 else f"{BASE_URL}/page/{page_num}"

            # ── robots.txt check ──
            if not is_allowed(rp, url):
                print(f"  [robots.txt] Blocked: {url} — stopping")
                break

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
    print("  MyJobMag scraper")
    print("=" * 50)
    print(f"Max pages  : {MAX_PAGES}")
    print(f"Max jobs   : ~{MAX_PAGES * 20:,}")
    print()

    # Check robots.txt before doing anything
    rp = load_robot_rules()

    if not is_allowed(rp, BASE_URL):
        print(f"\n[!] robots.txt disallows scraping {BASE_URL}")
        print("    Exiting out of respect for the site's rules.")
        exit(1)
    else:
        print(f"robots.txt check passed — /jobs is allowed\n")

    # Load existing URLs to skip already collected listings
    existing_urls = load_existing_urls()
    print(f"Existing listings in CSV : {len(existing_urls)}")
    print(f"These will be skipped.\n")

    # Scrape
    jobs = asyncio.run(scrape_all(existing_urls, rp))

    # Clean and save
    jobs = deduplicate(jobs)
    save_to_csv(jobs)

    print(f"\nTotal new listings added this run : {len(jobs)}")
    print(f"Run again any time to collect new postings.")