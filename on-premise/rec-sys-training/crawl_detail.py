import sys
import os
import time
import random
import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, as_completed

MIN_DELAY = 1.5
MAX_DELAY = 4
MAX_WORKERS = 2     
RETRY = 2
BATCH_SIZE = 1000      
BATCH_INDEX = int(sys.argv[1])

def random_delay():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

def init_driver():
    chrome_options = Options()

    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
    )

    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--remote-debugging-port=0")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-infobars")

    driver = webdriver.Chrome(options=chrome_options)
    return driver

def get_text_or_none(soup: BeautifulSoup, selector=None, attrs=None):
    tag = None
    if selector:
        tag = soup.select_one(selector)
    elif attrs:
        tag = soup.find(attrs=attrs)

    return tag.get_text(strip=True) if tag else None


def parse_hotel_page(soup: BeautifulSoup, url: str, hotel_id: int):

    # Location
    location_input = soup.find("input", {"name": "ss"})
    hotel_location = location_input.get("value") if location_input else None

    # Name
    hotel_name = get_text_or_none(
        soup,
        selector="h2.a4ac75716e.f546354b44.cc045b173b"
    )

    # Description
    desc_tag = soup.find("p", {"data-testid": "property-description"})
    hotel_description = desc_tag.get_text(separator=" ", strip=True) if desc_tag else None

    # Address
    address_tag = soup.find(
        "div",
        class_="b99b6ef58f cb4b7a25d9 b06461926f"
    )

    if address_tag:
        sub_div = address_tag.find("div")
        if sub_div:
            sub_div.extract()
        hotel_address = address_tag.get_text(strip=True)
    else:
        hotel_address = None

    return {
        "hotel_url": url,
        "hotel_location": hotel_location,
        "hotel_id": hotel_id,
        "hotel_name": hotel_name,
        "hotel_description": hotel_description,
        "hotel_address": hotel_address
    }

def crawl_worker(task):

    url, hotel_id = task
    driver = init_driver()

    try:
        for attempt in range(RETRY):
            try:
                logger.info(f"[Thread] Crawling hotel {hotel_id} (Attempt {attempt+1})")

                driver.get(url)
                random_delay()

                soup = BeautifulSoup(driver.page_source, "html.parser")
                return parse_hotel_page(soup, url, hotel_id)

            except Exception as e:
                logger.error(f"Error hotel {hotel_id}: {e}")
                random_delay()

        logger.error(f"Failed hotel {hotel_id}")
        return None

    finally:
        driver.quit()


if __name__ == "__main__":

    df_urls = pd.read_csv("data/url_hotels.csv")

    total_records = len(df_urls)
    start_idx = BATCH_INDEX * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, total_records)

    if start_idx >= total_records:
        logger.warning("Batch bigger record")
        exit()

    df_batch = df_urls.iloc[start_idx:end_idx]

    tasks = []
    for _, row in df_batch.iterrows():
        tasks.append((row["hotel_url"], row["hotel_id"]))

    results = []

    logger.info(f"Starting crawl with {MAX_WORKERS} threads...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [executor.submit(crawl_worker, task) for task in tasks]

        for future in as_completed(futures):
            data = future.result()
            if data:
                results.append(data)

    df_result = pd.DataFrame(results)

    os.makedirs("data/hotel_details", exist_ok=True)

    output_file = f"data/hotel_details/hotel_details_batch_{BATCH_INDEX}.csv"

    df_result.to_csv(output_file,
                     index=False,
                     encoding="utf-8")

    end_time = time.time()

    logger.info(f"Finished in {(end_time - start_time)/60:.2f} minutes")
    print(f"Done! Saved to data/hotel_details/hotel_details_batch_{BATCH_INDEX}.csv")

# python crawl_detail.py x (x: 0 -> 8)
