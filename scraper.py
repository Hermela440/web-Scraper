# Main scraping script for Job Listings Web Scraper

import os
import django
import sys

# Setup Django environment
sys.path.append(os.path.join(os.path.dirname(__file__), 'webapp'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'webapp.settings')
django.setup()

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from jobs.models import Job
from django.db import IntegrityError

URL = "https://remoteok.com/"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}

# --- CONFIGURABLE ---
KEYWORDS = ["python"]  # Filter jobs by these keywords (case-insensitive)
MAX_RETRIES = 3
BACKOFF_FACTOR = 1


def get_session():
    session = requests.Session()
    retries = Retry(total=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update(HEADERS)
    return session


def fetch_job_listings(url):
    session = get_session()
    try:
        response = session.get(url)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch jobs: {e}")
        return []
    # Try lxml, fallback to html.parser
    try:
        soup = BeautifulSoup(response.text, "lxml")
    except Exception:
        soup = BeautifulSoup(response.text, "html.parser")
    jobs = []
    for job_row in soup.find_all('tr', class_='job'):
        title = job_row.find('h2', itemprop='title')
        company = job_row.find('h3', itemprop='name')
        location = job_row.find('div', class_='location')
        date = job_row.find('time')
        link = job_row.get('data-href')
        if title and company and link:
            job_title = title.get_text(strip=True)
            # Keyword filter
            if KEYWORDS and not any(kw.lower() in job_title.lower() for kw in KEYWORDS):
                continue
            jobs.append({
                'Job Title': job_title,
                'Company': company.get_text(strip=True),
                'Location': location.get_text(strip=True) if location else 'Remote',
                'Date': date['datetime'] if date and date.has_attr('datetime') else '',
                'Link': f"https://remoteok.com{link}"
            })
    return jobs


def clean_jobs_data(jobs):
    df = pd.DataFrame(jobs)
    # Remove duplicates and clean up data
    df.drop_duplicates(inplace=True)
    df.fillna('N/A', inplace=True)
    return df


def save_jobs_to_csv(df, filename="jobs.csv"):
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} jobs to {filename}")


def save_jobs_to_excel(df, filename="jobs.xlsx"):
    df.to_excel(filename, index=False)
    print(f"Saved {len(df)} jobs to {filename}")


def save_jobs_to_sqlite(df, db_name="jobs.db"):
    conn = sqlite3.connect(db_name)
    df.to_sql('jobs', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Saved {len(df)} jobs to {db_name} (table: jobs)")


def save_jobs_to_django_db(df):
    created = 0
    for _, row in df.iterrows():
        try:
            Job.objects.get_or_create(
                title=row['Job Title'],
                company=row['Company'],
                location=row['Location'],
                date=row['Date'],
                link=row['Link'],
            )
            created += 1
        except IntegrityError:
            continue
    print(f"Saved {created} jobs to Django DB.")


def main():
    print(f"Fetching job listings from RemoteOK with keywords: {KEYWORDS}...")
    jobs = fetch_job_listings(URL)
    if not jobs:
        print("No jobs found.")
        return
    df = clean_jobs_data(jobs)
    save_jobs_to_csv(df)
    save_jobs_to_excel(df)
    save_jobs_to_sqlite(df)
    save_jobs_to_django_db(df)


if __name__ == "__main__":
    main() 