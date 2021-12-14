import unittest
import sys
import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
# myDir = os.getcwd()+"\\crawler"
# sys.path.append(myDir)
myDir = os.getcwd()
sys.path.append(myDir)
from crawler.spiders import ic_tpex
from crawler.items import CrawlerItem
from scrapy.http import Request as scrapy_request
import requests

from scrapy.http import HtmlResponse
INDUSTRY_PLATFORM = "https://ic.tpex.org.tw/index.php"
INDUSTRY_ROOT = "https://ic.tpex.org.tw/"

import pandas as pd


class TestIndustryChain(unittest.TestCase):
    def setUp(self):
        self.data = pd.read_json('crawler/tests/ic_tpex.json')
        #沒有設定regex=False，有一些數字會被吃掉
        self.data["company_code"]= self.data["company_code"].astype(str).str.replace(".0","",regex=False)
        self.industry_name = set(self.data["industry"])
        self.industry_id = self.data[["industry", "industry_id"]].drop_duplicates()
        self.spider = ic_tpex.IcTpexSpider()

    def test_get_industry_name_semiconductor(self):    
        self.assertIn('半導體', self.industry_name)

    def test_get_industry_name_solarenergy(self):
        self.assertIn('太陽能', self.industry_name)

    def test_get_industry_name_pharmaceutical(self):
        self.assertIn('製藥', self.industry_name)       

    def test_get_industry_id_of_semiconductor(self):
        filt = (self.industry_id["industry"]=='半導體')
        self.assertEqual('D000', self.industry_id.loc[filt]["industry_id"].values)

    def test_get_chain_stream_and_with_name(self):
        filt = (self.data["industry"]=='汽電共生') & (self.data["stream"]=='上游')
        result = self.data.loc[filt,['stream_name']].drop_duplicates().values
        self.assertEqual('設備製造業', result)

    def test_get_chain_stream_and_without_name(self):
        filt = (self.data["industry"]=='半導體') & (self.data["stream"]=='上游')
        result = self.data.loc[filt,['stream_name']].drop_duplicates().values
        self.assertEqual(None, result)

    def test_get_subindustry_names_row(self):
        filt = (self.data["industry"]=='汽電共生')
        subindustry_names_list = set(self.data.loc[filt]['industry_module'])
        self.assertIn('配電系統', subindustry_names_list)

    def test_get_chain_with_groups(self):
        filt = (self.data["industry"]=='智慧電網') & (self.data["industry_module"]=='高壓AMI')
        chain_group_list  = set(self.data.loc[filt]["chain_group"])
        self.assertIn(('智慧電表製造'), chain_group_list)

    def test_get_chain_without_groups(self):
        filt = (self.data["industry"]=='半導體')
        chain_group_list = self.data.loc[filt]["chain_group"].drop_duplicates().values
        self.assertEqual(None, chain_group_list)


    def test_get_no_chain_industries(self):
        filt = (self.data["industry"]=='休閒娛樂')
        no_chain_industry_name_list = set(self.data.loc[filt]['industry_module'])
        self.assertIn('高爾夫球具業', no_chain_industry_name_list)
    
    def test_get_company_market(self):
        filt = (self.data["industry"]=='半導體')
        market_list = {'本國上市公司', '外國上市公司', '本國上櫃公司', '本國興櫃公司', 
                       '外國上櫃公司', '知名外國企業', '創櫃公司'}
        row_title = set(self.data.loc[filt,["market"]]["market"].drop_duplicates())
        self.assertEqual(row_title, market_list)

    def test_get_company_name_without_subchain(self):
        filt = (self.data["industry"] =="半導體") & (self.data["industry_module"] == "IC模組")
        company_name_list = set(self.data.loc[filt]["company_name"])
        self.assertIn('台達電', company_name_list)

    def test_get_company_name_with_subchain(self):
        filt = (self.data["industry"]=='半導體') & (self.data["company_name"]=="台積電")
        company_name_list = set(self.data.loc[filt]["industry_module"])
        chain_group_list = {"晶圓製造", "DRAM製造", "其他IC/二極體製造"}
        self.assertEqual(chain_group_list, company_name_list)

    def test_get_company_url(self):
        filt = (self.data["industry"]=="半導體")
        company_url_list = set(self.data.loc[filt]["company_url"])
        self.assertIn("https://ic.tpex.org.tw/company_basic.php?stk_code=2308", company_url_list)

    def test_get_company_code(self):
        filt = (self.data["industry"]=="半導體")
        company_code_list = set(self.data.loc[filt]["company_code"])
        self.assertIn("2308", company_code_list)

    def test_parse(self):
        url = 'https://ic.tpex.org.tw/'
        response = requests.get(url)
        scrapy_response = HtmlResponse(url, body=response.content)
        results = self.spider.parse(scrapy_response)
        result_text = [str(link) for link in (links for links in results)]
        self.assertIn("<GET https://ic.tpex.org.tw/introduce.php?ic=D000>", result_text)

    def test_parse_content(self):
        url = 'https://ic.tpex.org.tw/introduce.php?ic=D000'
    
        meta_object={'categories':{
                                'industry':"半導體",
                                'industry_id': "D000"        
                    }
        }

        response = requests.get(url)
        scrapy_response = HtmlResponse(url, request = scrapy_request(url, meta = meta_object), body=response.content)
        results = self.spider.parse_content(scrapy_response)
        # result_text = [links for links in results]
        result_text = [CrawlerItem(link) for link in (links for links in results)]
        expected = CrawlerItem({"industry": "半導體", 
                    "industry_id": "D000", 
                    "stream": "上游", 
                    "stream_name": None, 
                    "industry_module": 
                    "IP設計/IC設計代工服務", 
                    "chain_group": {}, 
                    "market": "本國興櫃公司", 
                    "company_name": "芯測", 
                    "company_code": "6786", 
                    "business_id": "25089282", 
                    "company_url": "https://ic.tpex.org.tw/company_basic.php?stk_code=6786"})
        self.assertIn(expected, result_text)


if __name__ == '__main__':
    settings = get_project_settings()
    settings.set('FEEDS', {
                        'crawler/tests/ic_tpex.json': {
                        'format': 'json',
                        'encoding': 'utf-8',
                        "overwrite": True
                        }
                })
    settings.set( 'ITEM_PIPELINES', {
                                    'crawler.pipelines.CrawlerPipeline': 300,
                })

    
    process = CrawlerProcess(settings)
    process.crawl('ic_tpex')
    process.start()

    unittest.main()  
