# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    industry = scrapy.Field()
    industry_id = scrapy.Field()
    stream = scrapy.Field()
    stream_name = scrapy.Field()
    industry_module = scrapy.Field()
    chain_group = scrapy.Field()
    market = scrapy.Field()
    company_name = scrapy.Field()
    company_code = scrapy.Field()
    business_id = scrapy.Field()
    company_url = scrapy.Field()
    