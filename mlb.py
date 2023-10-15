import json
import math
import time
from typing import Dict, List

import requests
from loguru import logger as log
from nested_lookup import nested_lookup
from scrapfly import ScrapeApiResponse, ScrapeConfig, ScrapflyClient
import re

from decouple import config

SCRAPFLY = ScrapflyClient(key=config('SCRAPFLY_KEY'))
BASE_CONFIG = {
    "asp": True,
}
base_url="https://www.mlbshop.com/"
company_selector = 'li.entity-item>a'
filter_selector= 'a.side-nav-facet-item.hide-radio-button'
amount_selector='[data-talos="itemCount"]'


def parse_companies(base_url = base_url):
    log.info("scraping product {}", base_url)
    result = SCRAPFLY.scrape(ScrapeConfig(url=base_url))
    companies = [result.soup.select(company_selector)[i].attrs['href'] for i in
             range(0, len(result.soup.select(company_selector)))]
    return companies


def get_filters(company_link):
    log.info("scraping company filters {}", company_link)
    company_page=SCRAPFLY.scrape(ScrapeConfig(url=base_url + company_link))

    filters = [company_page.soup.select(filter_selector)[j].attrs['href']  for j
               in range(len(company_page.soup.select(filter_selector)))]

    return filters

def get_pages(filter):
    try:
        pattern = r'\d+'
        amount = SCRAPFLY.scrape(ScrapeConfig(url=base_url + filter)).soup.select(amount_selector)[0].text
        amount = int(re.findall(pattern, amount)[-1])
        page = math.ceil(amount / 72)
        return page
    except Exception as e:
        log.info(e)
        return 1

def parse_items(filter,page) -> List:
    items = []
    try:
        for i in range(0,page):
            items.append(SCRAPFLY.scrape(ScrapeConfig(url=base_url+filter +"?pageSize=72&pageNumber={}&sortOption=TopSellers".format(page))))
    except Exception as e:
        log.info(e)
    return items


async def scrape_items(url):
    log.info("scraping item {}", url)
    result = await SCRAPFLY.async_scrape(ScrapeConfig(str(url), **BASE_CONFIG))
    log.info("Requesting {} Status {}".format(url, result.status_code))
    product = {"urs":url,
                    "name":result.soup.select('''h1[data-talos="labelPdpProductTitle"]''')[0].text,
                    "slug": result.soup.select("span.breadcrumb-text")[0].text,
                    "price": result.soup.select('''div[class="layout-row pdp-price"]>div.price-card>div>div>span>span>span.money-value>span.sr-only''')[0].text,
                    "last_sale":"",
                    "gender":""}
    try:
        product["brand"]=result.soup.select('''body > div.layout-row > div > div:nth-child(6) > div.layout-column.large-4.medium-6.small-12 > div.layout-row.product-details > div > div.description-box-content > ul > li:nth-child(2)''')[0].text
    except:
        product["brand"] = "Failed to scrape"
    try:
        product["description"] =result.soup.select('''body > div.layout-row > div > div:nth-child(6) > div.layout-column.large-4.medium-6.small-12 > div.layout-row.product-description > div > div.description-box-content > div''')[0].text
    except:
        product["description"] = "Failed to scrape"
    try:
        product["category"] =result.soup.select('''body > div.layout-row > div > div.layout-row.pdp-style-breadcrumbs.pdp-breadcrumbs > div > ul > li:nth-child(2) > a''')[0].text
    except:
        product["category"] = "Failed to scrape"
    try:
        product["characteristics"] = {i:result.soup.select('''div.description-box-content>ul>li''')[i].text for i in range(len(result.soup.select('''div.description-box-content>ul>li''')))}
    except:
        product["characteristics"] = "Failed to scrape"
    try:
        product["images"] = result.soup.select('''div[class="carousel-container large-pdp-image"]>div>img''')[0].attrs['src']
    except:
        product["images"] = "Failed to scrape"
    try:
        product['variants'] = [j.text for j in result.soup.select('''a.size-selector-button.available''')]
    except:
        product['variants'] = "Failed to scrape"

    return product

