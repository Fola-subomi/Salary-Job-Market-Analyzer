import asyncio
import csv
import os
import random
from datetime import datetime
from playwright.async_api import async_playwright

BASE_URL = "https://ngojobsite.com"

# Max pages to scrape per run
# Each page has ~10-12 listings
MAX_PAGES = 30

OUTPUT_FILE = "data/raw/ngojobsite_raw.csv"
FIELDS = ["title", "company", "location", "date_posted", "url", "scraped_at"]

# Categories to skip — not actual job listings
SKIP_CATEGORIES = ["blog", "q&a", "ngo grants", "expatriate"]


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

def clean(text):
    """Remove extra whitespace and newlines."""
    return " ".join(text.split()) if text else ""


def split_title_company(full_title):
    """Split 'Job Title at Company Name' into separate fields.
    Handles edge cases like 'Manager at MSF (Médecins Sans Frontières)'
    """
    if " at " in full_title:
        parts = full_title.split(" at ", 1)
        return clean(parts[0]), clean(parts[1])
    return clean(full_title), ""


def is_nigeria_location(location):
    """Filter out non-Nigerian locations like 'Kenya', 'Syria' etc."""
    international = [
        "kenya", "ethiopia", "somalia", "syria", "jordan", "ukraine",
        "south sudan", "niger", "chad", "mali", "burkina faso", "senegal",
        "uganda", "rwanda", "tanzania", "ghana", "cameroon", "liberia",
        "sierra leone", "afghanistan", "pakistan", "bangladesh", "myanmar",
        "philippines", "indonesia", "colombia", "peru", "haiti", "fiji",
        "austria", "france", "germany", "switzerland", "netherlands",
        "norway", "sweden", "italy", "spain", "poland", "belgium",
        "united kingdom", "united states", "australia", "india", "egypt",
        "morocco", "algeria", "tunisia", "iraq", "lebanon", "occupied",
        "vanuatu", "solomon", "micronesia", "asia", "europe", "africa",
    ]
    loc_lower = location.lower()
    return not any(intl in loc_lower for intl in international)


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
        await page.wait_for_selector("div.dimasonrybox", timeout=15000)

        cards = await page.query_selector_all("div.dimasonrybox")

        for card in cards:
            try:
                # Title + company in h3.the-title a
                link_el = await card.query_selector("h3.the-title a")
                full_title = await link_el.inner_text() if link_el else ""
                href = await link_el.get_attribute("href") if link_el else ""

                title, company = split_title_company(clean(full_title))

                # Location — from category tag, skip "Nigeria" generic tag
                location = ""
                category_links = await card.query_selector_all("span.categoryurl a")
                for cat_link in category_links:
                    cat_text = clean(await cat_link.inner_text())
                    # Skip generic tags, keep state/city names
                    if cat_text.lower() not in ["nigeria", "nigeria ngo jobs"]:
                        location = cat_text
                        break

                # Skip non-Nigerian locations
                if location and not is_nigeria_location(location):
                    continue

                # Date posted
                date_el = await card.query_selector("span.post-date")
                date_posted = await date_el.inner_text() if date_el else ""

                if title:
                    jobs.append({
                        "title":       clean(title),
                        "company":     clean(company),
                        "location":    clean(location),
                        "date_posted": clean(date_posted),
                        "url":         href,
                        "scraped_at":  datetime.now().isoformat(),
                    })

            except Exception as e:
                print(f"    [!] Error parsing card: {e}")
                continue

        # Check if a next page exists
        next_btn = await page.query_selector("a.next.page-numbers")
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
            url = BASE_URL if page_num == 1 else f"{BASE_URL}/page/{page_num}/"
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
    print("  NgoJobSite scraper")
    print("=" * 50)
    print(f"Max pages : {MAX_PAGES}")
    print(f"Max jobs  : ~{MAX_PAGES * 12:,}")
    print()

    existing_urls = load_existing_urls()
    print(f"Existing listings in CSV : {len(existing_urls)}")
    print(f"These will be skipped.\n")

    jobs = asyncio.run(scrape_all(existing_urls))
    jobs = deduplicate(jobs)
    save_to_csv(jobs)

    print(f"\nTotal new listings added this run : {len(jobs)}")
    print(f"Run again any time to collect new postings.")