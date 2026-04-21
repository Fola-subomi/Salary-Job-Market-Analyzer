import asyncio
import csv
import os
import random
from datetime import datetime
from playwright.async_api import async_playwright

BASE_URL = "https://www.jobberman.com/jobs"

LOCATIONS = ["lagos", "abuja", "port-harcourt", "kano", "ibadan", "remote"]
JOB_TYPES = ["full-time", "part-time", "contract"]

# Max pages to scrape per location/job-type combo per run
# Raise this for a deeper scrape, lower it for a quick refresh
MAX_PAGES = 10

OUTPUT_FILE = "data/raw/jobberman_raw.csv"
FIELDS = ["title", "company", "location", "job_type", "salary", "sector",
          "url", "scraped_at"]


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

def clean(text):
    """Remove extra whitespace and newlines."""
    return " ".join(text.split()) if text else ""


def load_existing_urls():
    """Read URLs already saved in the CSV so we can skip them."""
    if not os.path.exists(OUTPUT_FILE):
        return set()
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["url"] for row in reader}


def save_to_csv(jobs):
    """Append new jobs to the CSV. Writes header only on first ever run."""
    os.makedirs("data/raw", exist_ok=True)
    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(jobs)
    print(f"\nDone. Appended {len(jobs)} new jobs to {OUTPUT_FILE}")


def deduplicate(jobs):
    """Remove duplicates within the current batch (title + company + location)."""
    seen = set()
    unique = []
    for job in jobs:
        key = (job["title"].lower(), job["company"].lower(), job["location"].lower())
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
    """Scrape all job cards from a single paginated page.
    Returns (jobs, has_next_page).
    """
    jobs = []
    has_next = False

    try:
        await page.goto(url, timeout=30000)
        await page.wait_for_selector("[data-cy='listing-title-link']", timeout=15000)

        cards = await page.query_selector_all("[data-cy='listing-title-link']")

        for link_el in cards:
            try:
                card = await link_el.evaluate_handle(
                    "el => el.closest('div.w-full')"
                )

                title_el = await link_el.query_selector("p")
                title = await title_el.inner_text() if title_el else ""

                href = await link_el.get_attribute("href") or ""

                company_el = await card.query_selector("p.text-sm.text-blue-700")
                company = await company_el.inner_text() if company_el else ""

                spans = await card.query_selector_all("span.mb-3.px-3.py-1.rounded")
                span_texts = [clean(await s.inner_text()) for s in spans]

                location = span_texts[0] if len(span_texts) > 0 else ""
                job_type = span_texts[1] if len(span_texts) > 1 else ""
                salary   = span_texts[2] if len(span_texts) > 2 else "Not specified"

                sector_el = await card.query_selector("p.text-sm.text-gray-500")
                sector = await sector_el.inner_text() if sector_el else ""

                if title:
                    jobs.append({
                        "title":      clean(title),
                        "company":    clean(company),
                        "location":   clean(location),
                        "job_type":   clean(job_type),
                        "salary":     clean(salary),
                        "sector":     clean(sector),
                        "url":        href,
                        "scraped_at": datetime.now().isoformat(),
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


async def scrape_location_jobtype(page, location, job_type, existing_urls):
    """Scrape all pages for a given location + job type up to MAX_PAGES."""
    all_jobs = []
    base = f"{BASE_URL}/{location}/{job_type}"

    for page_num in range(1, MAX_PAGES + 1):
        url = base if page_num == 1 else f"{base}?page={page_num}"
        print(f"  Page {page_num}: {url}")

        jobs, has_next = await scrape_page(page, url)

        # Filter out listings we already have
        new_jobs = [j for j in jobs if j["url"] not in existing_urls]
        all_jobs.extend(new_jobs)
        print(f"    {len(jobs)} listings found — {len(new_jobs)} new")

        # Stop early if whole page already collected
        if len(new_jobs) == 0:
            print(f"    All listings already collected — stopping early")
            break

        if not has_next:
            print(f"    No more pages after page {page_num}")
            break

        # Polite delay between pages
        await asyncio.sleep(random.uniform(1.5, 3))

    return all_jobs


async def scrape_all(existing_urls):
    """Loop through all location/job-type combos and collect new listings."""
    all_jobs = []
    total_combos = len(LOCATIONS) * len(JOB_TYPES)
    count = 0

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

        for location in LOCATIONS:
            for job_type in JOB_TYPES:
                count += 1
                print(f"\n[{count}/{total_combos}] {location} / {job_type}")

                jobs = await scrape_location_jobtype(
                    page, location, job_type, existing_urls
                )
                print(f"  Collected {len(jobs)} new jobs for {location}/{job_type}")
                all_jobs.extend(jobs)

                # Longer delay between combos
                await asyncio.sleep(random.uniform(3, 6))

        await browser.close()

    return all_jobs


# ─────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────

if __name__ == "__main__":
    total_combos = len(LOCATIONS) * len(JOB_TYPES)

    print("=" * 50)
    print("  Jobberman scraper")
    print("=" * 50)
    print(f"Locations : {', '.join(LOCATIONS)}")
    print(f"Job types : {', '.join(JOB_TYPES)}")
    print(f"Max pages : {MAX_PAGES} per combo")
    print(f"Max jobs  : ~{total_combos * MAX_PAGES * 16:,}")
    print()

    # Load what we already have so we only collect new listings
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