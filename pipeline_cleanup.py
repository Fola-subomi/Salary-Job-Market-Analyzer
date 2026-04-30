import os
import re
import pandas as pd

# ─────────────────────────────────────────
# File paths
# ─────────────────────────────────────────

RAW_DIR = "data/raw"
CLEAN_DIR = "data/clean"
OUTPUT_FILE = os.path.join(CLEAN_DIR, "jobs_clean.csv")

SOURCES = {
    "jobberman":       os.path.join(RAW_DIR, "jobberman_raw.csv"),
    "myjobmag":        os.path.join(RAW_DIR, "myjobmag_raw.csv"),
    "hotnigerianjobs": os.path.join(RAW_DIR, "hotnigerianjobs_raw.csv"),
    "ngojobsite":      os.path.join(RAW_DIR, "ngojobsite_raw.csv"),
    "kaggle":          os.path.join(RAW_DIR, "Job Posting.csv"),
}

# ─────────────────────────────────────────
# Location standardisation map
# ─────────────────────────────────────────

LOCATION_MAP = {
    "lagos state":      "Lagos",
    "lagos":            "Lagos",
    "abuja":            "Abuja",
    "fct":              "Abuja",
    "abuja, fct":       "Abuja",
    "port harcourt":    "Port Harcourt",
    "rivers state":     "Port Harcourt",
    "kano state":       "Kano",
    "kano":             "Kano",
    "ibadan":           "Ibadan",
    "oyo state":        "Ibadan",
    "remote":           "Remote",
    "remote jobs":      "Remote",
    "plateau state":    "Plateau State",
    "borno state":      "Borno State",
    "adamawa state":    "Adamawa State",
    "yobe state":       "Yobe State",
    "kaduna state":     "Kaduna State",
    "anambra state":    "Anambra State",
    "enugu state":      "Enugu State",
    "delta state":      "Delta State",
    "edo state":        "Edo State",
    "ogun state":       "Ogun State",
    "sokoto state":     "Sokoto State",
    "kebbi state":      "Kebbi State",
    "jigawa state":     "Jigawa State",
    "katsina state":    "Katsina State",
    "gombe state":      "Gombe State",
    "bauchi state":     "Bauchi State",
    "niger state":      "Niger State",
    "kwara state":      "Kwara State",
    "benue state":      "Benue State",
    "nasarawa state":   "Nasarawa State",
    "taraba state":     "Taraba State",
    "zamfara state":    "Zamfara State",
    "ekiti state":      "Ekiti State",
    "osun state":       "Osun State",
    "ondo state":       "Ondo State",
    "imo state":        "Imo State",
    "ebonyi state":     "Ebonyi State",
    "cross river state":"Cross River State",
    "akwa ibom state":  "Akwa Ibom State",
    "bayelsa state":    "Bayelsa State",
    "abia state":       "Abia State",
    "kogi state":       "Kogi State",
}


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

def clean_text(text):
    """Strip whitespace and normalise."""
    if pd.isna(text):
        return ""
    return " ".join(str(text).split()).strip()


def standardise_location(location):
    """Map raw location strings to standardised city/state names."""
    if pd.isna(location) or location == "":
        return "Unknown"
    loc = location.lower().strip()
    return LOCATION_MAP.get(loc, location.title())


def extract_salary(salary_str):
    """Extract min and max salary from strings like 'NGN 150,000 - 250,000'.
    Returns (min_salary, max_salary) as floats or (None, None) if not parseable.
    """
    if pd.isna(salary_str) or salary_str in ["", "Not specified", "Confidential"]:
        return None, None

    # Remove currency symbols and labels
    cleaned = re.sub(r"[NGN₦,]", "", str(salary_str), flags=re.IGNORECASE).strip()

    # Find all numbers in the string
    numbers = re.findall(r"\d+(?:\.\d+)?", cleaned)
    numbers = [float(n) for n in numbers]

    if len(numbers) == 0:
        return None, None
    elif len(numbers) == 1:
        return numbers[0], numbers[0]
    else:
        return min(numbers), max(numbers)


def clean_title(title):
    """Remove noise from job titles."""
    if pd.isna(title):
        return ""
    # Remove bracketed suffixes like (BGH), (BSN), (2), (Jos)
    title = re.sub(r"\s*\([^)]*\)\s*$", "", str(title)).strip()
    # Remove trailing numbers like "- 2", "- 3"
    title = re.sub(r"\s*-\s*\d+$", "", title).strip()
    return clean_text(title)


def clean_company(company):
    """Normalise company names."""
    if pd.isna(company):
        return ""
    company = str(company)
    # Remove "Anonymous" variants
    if "anonymous" in company.lower():
        return "Anonymous"
    return clean_text(company)


# ─────────────────────────────────────────
# Source loaders — one per source
# ─────────────────────────────────────────

def load_jobberman(path):
    df = pd.read_csv(path)
    df = df.rename(columns={
        "title":    "title",
        "company":  "company",
        "location": "location",
        "job_type": "job_type",
        "salary":   "salary_raw",
        "sector":   "sector",
        "url":      "url",
    })
    df["source"] = "jobberman"
    df["date_posted"] = ""
    return df[["title", "company", "location", "job_type",
               "salary_raw", "sector", "date_posted", "url", "source"]]


def load_myjobmag(path):
    df = pd.read_csv(path)
    df = df.rename(columns={
        "title":       "title",
        "company":     "company",
        "date_posted": "date_posted",
        "url":         "url",
    })
    df["source"]     = "myjobmag"
    df["location"]   = ""
    df["job_type"]   = ""
    df["salary_raw"] = ""
    df["sector"]     = ""
    return df[["title", "company", "location", "job_type",
               "salary_raw", "sector", "date_posted", "url", "source"]]


def load_hotnigerianjobs(path):
    df = pd.read_csv(path)
    df = df.rename(columns={
        "title":       "title",
        "company":     "company",
        "date_posted": "date_posted",
        "url":         "url",
    })
    df["source"]     = "hotnigerianjobs"
    df["location"]   = ""
    df["job_type"]   = ""
    df["salary_raw"] = ""
    df["sector"]     = ""
    return df[["title", "company", "location", "job_type",
               "salary_raw", "sector", "date_posted", "url", "source"]]


def load_ngojobsite(path):
    df = pd.read_csv(path)
    df = df.rename(columns={
        "title":       "title",
        "company":     "company",
        "location":    "location",
        "date_posted": "date_posted",
        "url":         "url",
    })
    df["source"]     = "ngojobsite"
    df["job_type"]   = ""
    df["salary_raw"] = ""
    df["sector"]     = "NGO / Humanitarian"
    return df[["title", "company", "location", "job_type",
               "salary_raw", "sector", "date_posted", "url", "source"]]


def load_kaggle(path):
    df = pd.read_csv(path)

    # Rename Kaggle columns to match our schema
    # Adjust these if your Kaggle file has different column names
    rename_map = {}
    cols = df.columns.str.lower().tolist()

    if "job opening title" in cols:
        rename_map[df.columns[cols.index("job opening title")]] = "title"
    if "website domain" in cols:
        rename_map[df.columns[cols.index("website domain")]] = "company"
    if "location" in cols:
        rename_map[df.columns[cols.index("location")]] = "location"
    if "category" in cols:
        rename_map[df.columns[cols.index("category")]] = "sector"
    if "seniority" in cols:
        rename_map[df.columns[cols.index("seniority")]] = "job_type"
    if "job opening url" in cols:
        rename_map[df.columns[cols.index("job opening url")]] = "url"
    if "first seen" in cols:
        rename_map[df.columns[cols.index("first seen")]] = "date_posted"

    df = df.rename(columns=rename_map)

    # Fill missing columns
    for col in ["title", "company", "location", "job_type",
                "sector", "date_posted", "url"]:
        if col not in df.columns:
            df[col] = ""

    df["source"]     = "kaggle"
    df["salary_raw"] = ""

    return df[["title", "company", "location", "job_type",
               "salary_raw", "sector", "date_posted", "url", "source"]]


# ─────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────

def run_pipeline():
    os.makedirs(CLEAN_DIR, exist_ok=True)
    frames = []

    loaders = {
        "jobberman":       load_jobberman,
        "myjobmag":        load_myjobmag,
        "hotnigerianjobs": load_hotnigerianjobs,
        "ngojobsite":      load_ngojobsite,
        "kaggle":          load_kaggle,
    }

    # ── Load all sources ──
    print("Loading sources...")
    for name, loader in loaders.items():
        path = SOURCES[name]
        if not os.path.exists(path):
            print(f"  [{name}] Not found — skipping")
            continue
        try:
            df = loader(path)
            print(f"  [{name}] Loaded {len(df):,} rows")
            frames.append(df)
        except Exception as e:
            print(f"  [{name}] Error loading: {e}")

    if not frames:
        print("\nNo data loaded. Run the scrapers first.")
        return

    # ── Merge ──
    print("\nMerging all sources...")
    df = pd.concat(frames, ignore_index=True)
    print(f"  Total rows before cleaning: {len(df):,}")

    # ── Clean columns ──
    print("\nCleaning columns...")
    df["title"]    = df["title"].apply(clean_title)
    df["company"]  = df["company"].apply(clean_company)
    df["location"] = df["location"].apply(standardise_location)
    df["sector"]   = df["sector"].apply(clean_text)
    df["job_type"] = df["job_type"].apply(clean_text)

    # ── Extract salary ──
    print("Extracting salary ranges...")
    df[["min_salary", "max_salary"]] = df["salary_raw"].apply(
        lambda x: pd.Series(extract_salary(x))
    )
    df["avg_salary"] = df[["min_salary", "max_salary"]].mean(axis=1)

    # ── Drop rows with no title ──
    before = len(df)
    df = df[df["title"].str.len() > 0]
    print(f"  Dropped {before - len(df)} rows with empty titles")

    # ── Deduplicate ──
    before = len(df)
    df = df.drop_duplicates(subset=["title", "company", "location"])
    print(f"  Dropped {before - len(df)} duplicate rows")

    # ── Final column order ──
    df = df[[
        "title", "company", "location", "job_type", "sector",
        "min_salary", "max_salary", "avg_salary", "salary_raw",
        "date_posted", "url", "source"
    ]]

    df = df.reset_index(drop=True)

    # ── Save ──
    df.to_csv(OUTPUT_FILE, index=False)

    # ── Summary ──
    print(f"\n{'=' * 50}")
    print(f"  Clean dataset saved to {OUTPUT_FILE}")
    print(f"{'=' * 50}")
    print(f"Total rows            : {len(df):,}")
    print(f"Rows with salary data : {df['avg_salary'].notna().sum():,}")
    print(f"Unique locations      : {df['location'].nunique()}")
    print(f"Unique sectors        : {df['sector'].nunique()}")
    print(f"Unique job titles     : {df['title'].nunique():,}")
    print(f"\nRows per source:")
    print(df["source"].value_counts().to_string())
    print(f"\nSalary coverage by source:")
    print(df.groupby("source")["avg_salary"].apply(
        lambda x: f"{x.notna().sum()} / {len(x)} rows have salary"
    ).to_string())


if __name__ == "__main__":
    run_pipeline()