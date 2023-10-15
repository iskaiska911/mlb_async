import asyncio
import threading
import time
import threading
from mlb import parse_companies,get_filters,get_pages,scrape_items
from pathlib import Path
import numpy as np
from decouple import config
import queue
from scrapfly import ScrapeApiResponse, ScrapeConfig, ScrapflyClient
#import stockx
from tools import post_products_mlb
import httpx
import asyncio
from time import time,sleep


output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

SERVER_NUMBER = int(config('SERVER_NUMBER'))
NUM_PROCESSES = int(config('NUM_PROCESSES'))

SCRAPFLY = ScrapflyClient(key=config('SCRAPFLY_KEY'), max_concurrency=20)
BASE_CONFIG = {
    "asp": True,
    "country":"US"
}


price_selector = 'div[class="layout-row pdp-price"]>div.price-card>div>div>span>span>span.money-value>span.sr-only'
title_selector = 'h1[data-talos="labelPdpProductTitle"]'
size_selector = 'a.size-selector-button.available'
image_selector = 'div[class="carousel-container large-pdp-image"]>div>img'

base_url="https://www.mlbshop.com/"
df_list=[]

def run_async_scrape_item(url, result_queue):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # loop = asyncio.get_event_loop()
    sleep(0.4)
    product = loop.run_until_complete(scrape_items(url))
    result_queue.put(product)


async def run():
    BASE_CONFIG["cache"] = True
    companies = parse_companies()
    final_items_links = []
    for company in companies[0:1]: #убрать ограничение
        _first_start = time()
        company_index=companies.index(company)
        filters = list(set(get_filters(company)))
        for filter_element in filters:
            filter_index=filters.index(filter_element)
            max_page = get_pages(filter_element)
            items_links = []
            for i in range(1, max_page): #посавить max_page
                print(f"scraping for company {company_index} out of {len(companies)}\n"
                      f"scraping filter {filter_index} out of {len(filters)}\n"
                      f"page {i} out of {max_page}")
                _start = time()
                url = base_url + filter_element + "?pageSize=72&pageNumber={}&sortOption=TopSellers".format(i)
                items_links.append(await SCRAPFLY.async_scrape(ScrapeConfig(url=url)))
                print(f"finished scraping page number {i} in {time() - _start:.2f} seconds")
                final_items_links.append(['https://www.mlbshop.com'+items_links[i].soup.select('div.product-image-container>a')[j].attrs['href'] for i in
                         range(0, len(items_links)) for j in
                         range(0, len(items_links[i].soup.select('div.product-image-container>a')))])
                print(len(final_items_links))
        print(f"finished scraping links by one company in {((time() - _first_start)/60):.2f} mins")


    final_items_links=list(set([item for sublist in final_items_links for item in sublist]))
    print(f"###############----amount of requests {len(final_items_links)}----###############")

    formatted_items_links = np.array_split(final_items_links, len(final_items_links) / NUM_PROCESSES)

    for items_link_parts in formatted_items_links:
        result_queue = queue.Queue()
        threads = []
        for i in items_link_parts:
            thread = threading.Thread(target=run_async_scrape_item, args=(i, result_queue))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())


        post_products_mlb(results)
        print("All treads have completed successfully")


if __name__ == "__main__":
    asyncio.run(run())
