import os
import time

import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

WAIT_TIMEOUT = 10

url_default = 'https://www.booking.com/searchresults.html?group_adults=2&no_rooms=1&group_children=0&order=class&lang=en-us'
dest_name = ['Hà Nội', 'Đà Nẵng', 'Hội An', 'Ninh Bình', 'TP. Hồ Chí Minh', 'Nha Trang', 'Phú Quốc', 'Huế', 'Đà Lạt', 'Vũng Tàu']
dest_id = ['3714993', '3712125', '3715584', '3724181', '3730078', '3723998', '3726177', '3715887', '3712045', '3733750']

list_url_page = []

for i in range(len(dest_id)):
    url_page = f'{url_default}&dest_id=-{dest_id[i]}&dest_type=city'
    list_url_page.append(url_page)

print(len(list_url_page))

chrome_options = Options()
chrome_options.add_argument(
    "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
)
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--incognito")
driver = webdriver.Chrome(options=chrome_options)

def close_popup_if_exists():
    try:
        close_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[@aria-label='Dismiss sign-in info.']")
            )
        )
        close_button.click()
        logger.info("Closed login pop up")
    except TimeoutException:
        logger.info("No login popup found")

def crawl_list_hotel_in_page(url: str):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(WAIT_TIMEOUT)

    while True:
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(WAIT_TIMEOUT)
            show_more_xpath = "//button[.//span[contains(text(), 'Load more results')]]"
        
            show_more_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, show_more_xpath))
            )

            driver.execute_script("arguments[0].click();", show_more_button)
            logger.info("Click `Load more results` button")
            time.sleep(2)
        except TimeoutException:
            logger.info("No more `Load more results` button → Stop clicking")
            break

    time.sleep(WAIT_TIMEOUT)

    page = BeautifulSoup(driver.page_source, features="html.parser")

    tags = page.find_all('a', attrs={'data-testid': 'title-link'})

    url_hotels = [tag.get('href') for tag in tags]

    return url_hotels

driver.get(list_url_page[0])
time.sleep(5)

close_popup_if_exists()

list_url_hotels = []

for url in list_url_page:
    logger.info(f"Crawling: {url}")
    driver.get(url)
    time.sleep(5)
    close_popup_if_exists()
    list_url_hotels.extend(crawl_list_hotel_in_page(url))

list_url_hotels = list(dict.fromkeys(list_url_hotels))

print(f"Total hotels: {len(list_url_hotels)}")

df = pd.DataFrame({
    "hotel_id": range(1, len(list_url_hotels) + 1),
    "hotel_url": list_url_hotels
})

os.makedirs("data", exist_ok=True)
dest = os.path.join("data", "url_hotels.csv")

df.to_csv(dest, index=False, encoding="utf-8")

