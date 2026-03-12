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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

MIN_DELAY = 1.5
MAX_DELAY = 4
MAX_WORKERS = 5    
RETRY = 2
BATCH_SIZE = 100      
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
    chrome_options.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=chrome_options)
    return driver

def get_text_or_none(soup: BeautifulSoup, selector=None, attrs=None):
    tag = None
    if selector:
        tag = soup.select_one(selector)
    elif attrs:
        tag = soup.find(attrs=attrs)

    return tag.get_text(strip=True) if tag else None


def parse_ratings_page(soup: BeautifulSoup, hotel_id: int):

    reviews = []

    review_blocks = soup.find_all("div", attrs={"data-testid": "review-card"})

    for review in review_blocks:
        try:
            # User name
            user_tag = review.find("div", class_="b08850ce41 f546354b44")
            user_name = user_tag.get_text(strip=True) if user_tag else None

            # Country
            country_tag = review.find("span", class_="d838fb5f41 aea5eccb71")
            country = country_tag.get_text(strip=True) if country_tag else None

            # Review title
            title_tag = review.find("h4", attrs={"data-testid": "review-title"})
            review_title = title_tag.get_text(strip=True) if title_tag else None

            # Rating
            rating_tag = review.find("div", class_="f63b14ab7a dff2e52086")
            rating = rating_tag.get_text(strip=True) if rating_tag else None

            reviews.append({
                "HotelID": hotel_id,
                "User": user_name,
                "Country": country,
                "Review": review_title,
                "Rating": rating
            })

        except Exception as e:
            logger.warning(f"Error parsing review: {e}")

    return reviews

def crawl_all_review_pages(driver, hotel_id):
    all_reviews = []
    page_number = 1

    while True:
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        reviews = parse_ratings_page(soup, hotel_id)

        if len(reviews) == 0:
            logger.warning(f"Page {page_number} returned 0 reviews → trying Show all reviews")

            try:
                show_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//button[.//span[contains(text(),'Show all reviews')]]")
                    )
                )
                driver.execute_script("arguments[0].click();", show_btn)
                time.sleep(3)

                soup = BeautifulSoup(driver.page_source, "html.parser")
                reviews = parse_ratings_page(soup, hotel_id)

                logger.info(f"After clicking Show all. {len(reviews)} reviews")

            except:
                logger.info("No Show all button found")
            
        if len(reviews) == 0:
            logger.info("No more real reviews. Stop")
            break
        
        all_reviews.extend(reviews)

        logger.info(f"Parsed page {page_number} - {len(reviews)} reviews")

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        next_page_number = page_number + 1

        try:
            next_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.XPATH, f"//button[@aria-label=' {next_page_number}']")
                )
            )

            driver.execute_script("arguments[0].click();", next_button)
            logger.info(f"Clicked page {next_page_number}")

            page_number += 1
            time.sleep(1)

        except:
            logger.info("No more review pages")
            break

    return all_reviews

def crawl_worker(task):
    url, hotel_id = task
    driver = init_driver()
    
    try:
        for attempt in range(RETRY):
            try:
                logger.info(f"[Thread] Crawling hotel {hotel_id} (Attempt {attempt+1})")
                driver.get(url)
                
                wait = WebDriverWait(driver, 10)
                try:
                    reviews_tab = wait.until(EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, 'a[data-testid="Property-Header-Nav-Tab-Trigger-reviews"]')
                    ))
                    
                    driver.execute_script("arguments[0].click();", reviews_tab)
                    logger.info(f"Clicked reviews tab for hotel {hotel_id}")
                    
                    time.sleep(1) 
                    
                except Exception as e:
                    logger.warning(f"Could not click reviews tab for {hotel_id}: {e}")

                return crawl_all_review_pages(driver, hotel_id)


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
        logger.warning("Batch bigger record.")
        exit()

    df_batch = df_urls.iloc[start_idx:end_idx]

    tasks = [(row["hotel_url"], idx + 1)
             for idx, row in df_batch.iterrows()]

    results = []

    logger.info(f"Starting crawl with {MAX_WORKERS} threads...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [executor.submit(crawl_worker, task) for task in tasks]

        for future in as_completed(futures):
            data = future.result()
            if data:
                results.extend(data)

    df = pd.DataFrame(results)


    master_path = "data/hotel_ratings/user_master.csv"

    if os.path.exists(master_path):
        df_master = pd.read_csv(master_path)
    else:
        df_master = pd.DataFrame(columns=["UserID", "User"])

    existing_users = set(df_master["User"].values)
    new_users = set(df["User"].dropna().unique()) - existing_users

    if len(df_master) > 0:
        next_id = df_master["UserID"].max() + 1
    else:
        next_id = 1
    
    new_user_rows = []
    for user in new_users:
        new_user_rows.append({
            "UserID": next_id,
            "User": user
        })
        next_id += 1

    df_new_users = pd.DataFrame(new_user_rows)

    df_master = pd.concat([df_master, df_new_users], ignore_index=True)

    df_master.to_csv(master_path, index=False, encoding="utf-8")

    user_id_map = dict(zip(df_master["User"], df_master["UserID"]))
    df["UserID"] = df["User"].map(user_id_map)
    df = df[['HotelID', 'UserID', 'User', 'Country', 'Review', 'Rating']]

    os.makedirs("data/hotel_ratings", exist_ok=True)

    output_file = f"data/hotel_ratings/hotel_ratings_batch_{BATCH_INDEX}.csv"

    df.to_csv(output_file,
                     index=False,
                     encoding="utf-8")

    end_time = time.time()

    logger.info(f"Finished in {(end_time - start_time)/60:.2f} minutes")
    print(f"Done! Saved to data/hotel_ratings/hotel_ratings_batch_{BATCH_INDEX}.csv")

# python crawl_ratings.py x (x: 0 -> 81)
