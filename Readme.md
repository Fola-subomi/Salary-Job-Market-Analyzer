# Nigerian Job Market Analyzer

A full-stack data science project that scrapes Nigerian job boards, cleans and structures the data, and delivers an interactive dashboard with a salary prediction tool — built to understand the Nigerian job market from real, self-collected data.

---

## Live Demo

> Coming soon — will be deployed on Streamlit Community Cloud

---

## Project Overview

Most salary and job market datasets focus on the US or Europe. This project collects real, up-to-date job data directly from Nigerian job boards and uses it to answer questions like:

- What are the most in-demand roles in Lagos right now?
- What salary should a mid-level accountant in Abuja expect?
- Which sectors are hiring the most?
- How do salaries differ across Nigerian cities?

---

## Features

- **Multi-source scraper** — collects job listings from Jobberman and MyJobMag
- **Incremental scraping** — only collects new listings on each run, no duplicates
- **Robots.txt compliance** — respects each site's scraping rules before touching anything
- **Data cleaning pipeline** — normalises salary strings, deduplicates, and structures raw data
- **Exploratory dashboard** — interactive filters by city, sector, and job type
- **Salary predictor** — enter a role, city, and experience level to get a salary estimate

---

## Tech Stack

| Layer | Tools |
|---|---|
| Scraping | Python, Playwright |
| Data pipeline | Pandas, SQLite |
| Machine learning | scikit-learn (Random Forest) |
| Visualisation | Plotly |
| Dashboard | Streamlit |
| Deployment | Streamlit Community Cloud |

---

## Project Structure

```
nigerian-job-market-analyzer/
│
├── data/
│   ├── raw/                        # Raw scraped CSVs (gitignored)
│   │   ├── jobberman_raw.csv
│   │   └── myjobmag_raw.csv
│   └── clean/                      # Cleaned, merged dataset (gitignored)
│       └── jobs_clean.csv
│
├── scrapers/
│   ├── jobberman.py                # Jobberman scraper
│   └── myjobmag.py                 # MyJobMag scraper
│
├── pipeline/
│   └── clean.py                    # Data cleaning and merging script
│
├── model/
│   ├── train.py                    # Train and evaluate the salary model
│   └── salary_model.pkl            # Saved trained model
│
├── app/
│   └── dashboard.py                # Streamlit dashboard + predictor
│
├── notebooks/
│   └── eda.ipynb                   # Exploratory data analysis notebook
│
├── requirements.txt                # Python dependencies
├── .gitignore                      # Excludes raw data and sensitive files
└── README.md                       # This file
```

---

## Getting Started

### 1. Clone the repo

```bash
git clone https://github.com/yourusername/nigerian-job-market-analyzer.git
cd nigerian-job-market-analyzer
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 3. Run the scrapers

```bash
python scrapers/jobberman.py
python scrapers/myjobmag.py
```

Raw data will be saved to `data/raw/`.

### 4. Clean the data

```bash
python pipeline/clean.py
```

### 5. Train the salary model

```bash
python model/train.py
```

### 6. Launch the dashboard

```bash
streamlit run app/dashboard.py
```

---

## Data Sources

| Source | Coverage | Fields collected |
|---|---|---|
| [Jobberman](https://www.jobberman.com) | Lagos, Abuja, Port Harcourt, Kano, Ibadan, Remote | Title, company, location, job type, salary, sector |
| [MyJobMag](https://www.myjobmag.com) | Nigeria-wide | Title, company, date posted |
with combination of kaggle dataset for robustness
https://www.kaggle.com/datasets/techsalerator/job-posting-data-in-nigeria

> Raw data files are excluded from this repo via `.gitignore` out of respect for the source websites. Run the scrapers locally to generate your own dataset.

---

## How the Salary Predictor Works

The predictor uses a **Random Forest regression model** trained on the cleaned dataset. Features fed into the model:

- Job title (encoded)
- City / location
- Sector
- Job type (full-time, part-time, contract)

The model outputs a predicted monthly salary range in NGN. Performance is evaluated using Mean Absolute Error (MAE) and R².

---

## Roadmap

- [x] Jobberman scraper with pagination and incremental updates
- [x] MyJobMag scraper
- [ ] Data cleaning pipeline
- [ ] Exploratory analysis notebook
- [ ] Salary prediction model
- [ ] Streamlit dashboard
- [ ] Deployment to Streamlit Cloud
- [ ] NgCareers scraper (v2)
- [ ] NLP on job descriptions (v2)

---

## Ethical Scraping

This project is built with responsible scraping practices:

- Robots.txt is checked and respected before any page is requested
- Random delays are added between requests to avoid overloading servers
- Raw data is not redistributed — run the scrapers yourself to generate the dataset
- Data is used solely for personal portfolio and research purposes

---

## Author

Built by Precious Akogun as a portfolio project to explore the Nigerian job market using real data.