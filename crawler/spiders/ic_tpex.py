import scrapy
from ..items import CrawlerItem
import copy
from collections import defaultdict
import json
import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup

class IcTpexSpider(scrapy.Spider):
    name = 'ic_tpex'
    allowed_domains = ['ic.tpex.org.tw']
    start_urls = ['https://ic.tpex.org.tw/']

    with open('data/business_id_map.json') as f:
        business_id_map = json.load(f)

    INDUSTRY_PLATFORM = "https://ic.tpex.org.tw/index.php"
    INDUSTRY_ROOT = "https://ic.tpex.org.tw/"
    STCOK_BUSINESS = "https://goodinfo.tw/StockInfo/BasicInfo.asp?STOCK_ID="
    stat_stream = {}
    def parse(self, response):
        soup = BeautifulSoup(response.text, "html.parser")
        categories = CrawlerItem()
        industry_tags = self.get_all_industry_tags(soup)
        for industry_tag in industry_tags[:-1]:
            industry_name = self.get_industry_name(industry_tag)
            href = self.get_industry_href(industry_tag)
            
            # 綠色能源
            if href == 'javascript:;':
                sub_industry_tags = self.get_sub_industry_tags(industry_tag)
                for sub_industry_tag in sub_industry_tags:
                    sub_industry_infos = sub_industry_tag.find_all('span')
                    
                    for i, sub_industry_info in enumerate(sub_industry_infos[1:]):
                        if i==0:
                            sub_industry_name = sub_industry_info.text
                            continue

                        else:
                            third_industry_name = sub_industry_info.text[2:]
                            third_industry_href = sub_industry_info.get('onclick').replace("location.href='","").replace("'", "")

                        categories['industry'] = third_industry_name
                        href = self.INDUSTRY_ROOT + third_industry_href
                        categories['industry_id'] = self.get_industry_id(third_industry_href)

                        yield scrapy.Request(href, meta={'categories': copy.deepcopy(categories)}, callback=self.parse_content)

            # 生技醫療
            elif href == 'javascript:void(0);':
                sub_industry_tags = self.get_sub_industry_tags(industry_tag)
                for sub_industry_tag in sub_industry_tags:
                    sub_industry_name = sub_industry_tag.text
                    sub_industry_href = sub_industry_tag.find('a').get('href')

                    categories['industry'] = sub_industry_name
                    href = self.INDUSTRY_ROOT + sub_industry_href
                    categories['industry_id'] = self.get_industry_id(sub_industry_href)

                    yield scrapy.Request(href, meta={'categories': copy.deepcopy(categories)}, callback=self.parse_content)

            else:
                categories['industry'] = industry_name
                href = self.INDUSTRY_ROOT + href
                categories['industry_id'] = self.get_industry_id(href)

    
                yield scrapy.Request(href, meta={'categories': copy.deepcopy(categories)}, callback=self.parse_content)

    def parse_content(self, response):
      
        # breakpoint()
        soup = BeautifulSoup(response.text, "html.parser")
        industry_chain_overview = self.get_industry_chain_overview(soup)
        chains = self.get_all_chains(industry_chain_overview)
        Stream_dict = defaultdict(dict)
        industry=response.meta["categories"]["industry"]
        self.logger.info(f'爬"{industry}"產業中')
        if chains!=[]:
            
            for chain in chains:
                # 建立子模塊上中下游dict
                chain_stream, chain_stream_name = self.get_chain_stream_and_name(chain)
                Stream_dict[industry][chain_stream] = defaultdict(dict)
                Stream_dict[industry][chain_stream]['stream_name'] = chain_stream_name

                subindustry_names_row = self.get_subindustry_names_row(chain)
                subindustry_ids = [subindustry_name_row.get('id').split('_')[-1] for subindustry_name_row in subindustry_names_row]
                subindustry_names = [subindustry_name_row.text for subindustry_name_row in subindustry_names_row]

                for subindustry_id, subindustry_name in zip(subindustry_ids, subindustry_names):
                    Stream_dict[industry][chain_stream][subindustry_name] = defaultdict(dict)
                    self.stat_stream[subindustry_id] = (chain_stream, subindustry_name)
                    Stream_dict[industry][chain_stream][subindustry_name]['chain-group'] = {}


                # 處裡chain-group，如風力發電、汽電共生、智慧電網、汽車
                chain_groups = self.get_chain_groups(chain)
                if chain_groups:
                    chain_group_list = []
                    for chain_group in chain_groups:
                        try:
                            chain_group_title = chain_group.find('h4').text
                        except:
                            continue
                        chain_group_industry_names_row = self.get_subindustry_names_row(chain_group)
                        chain_group_industry_names = [chain_group_industry_name_row.text for chain_group_industry_name_row in chain_group_industry_names_row]
                        for subindustry_name in chain_group_industry_names:
                            Stream_dict[industry][chain_stream][subindustry_name]['chain-group'] = chain_group_title


        # 無產業上下游
        else:
            # 網頁寫超爛，class參數一直改
            no_chain_industries = self.get_no_chain_industries(industry_chain_overview)
            Stream_dict[industry]['無上中下游'] = defaultdict(dict)
            for no_chain_industry in no_chain_industries:
                no_chain_industry_div = no_chain_industry.find('div', {"class": "company-chain-panel"})
                no_chain_industry_id = no_chain_industry_div.get('id').split('_')[-1]
                no_chain_industry_name = no_chain_industry_div.text
                Stream_dict[industry]['無上中下游'][no_chain_industry_name] = defaultdict(dict)
                self.stat_stream[no_chain_industry_id] = ('無上中下游', no_chain_industry_name)
            

        self.get_all_company_info(response.meta["categories"]["industry"], Stream_dict, industry_chain_overview)
        industryItem = CrawlerItem()
        industryItem["industry"] = industry
        industryItem["industry_id"] = response.meta["categories"]["industry_id"]
        streams = Stream_dict[industry].keys()
        streams = list(streams)
        for stream in streams:
            if "\n" in stream:
                industryItem["stream"] = stream.split("\n")[1]
            else:
                industryItem["stream"] = stream
            industryItem["stream_name"] = Stream_dict[industry][stream]['stream_name']
            industry_modules = Stream_dict[industry][stream].keys()
            industry_modules = list(industry_modules)
            try:
                industry_modules.remove('stream_name')
            except:
                pass
            if not industry_modules:
                continue

            for industry_modules in industry_modules:
                industryItem["industry_module"] = industry_modules
                industryItem["chain_group"] = Stream_dict[industry][stream][industry_modules]["chain-group"]
                markets = Stream_dict[industry][stream][industry_modules].keys()
                markets = list(markets)
                try:
                    markets.remove("chain-group")
                except:
                    pass
                for market in markets:
                    industryItem["market"] = market
                    for company in Stream_dict[industry][stream][industry_modules][market]:
                        industryItem["company_name"] = company[0]
                        industryItem["company_code"] = company[1]
                        industryItem["business_id"] = company[2]
                        industryItem["company_url"] = company[3]
                        yield industryItem

    def request_the_url(url):
        response = rerequest_the_urlquests.get(url)
        response.encoding = 'utf8'
        return response
    def get_all_industry_tags(self,soup):
        return soup.find_all('div', {"class": "item"})


    def get_industry_name(self,industry_tag):
        return industry_tag.find('span', {"class": "txt"}).text


    def get_industry_href(self,industry_tag):
        return industry_tag.find('a').get('href')


    def get_sub_industry_tags(self,industry_tag):
        return industry_tag.find('ul').find_all('li', {"class": "listItem"})


    def get_industry_id(self,href):
        return href.split('=')[-1]

    def get_industry_chain_overview(self,soup):
        return soup.find('div', {"class": "content"})


    def get_all_chains(self,industry_chain_overview):
        return industry_chain_overview.find_all('div', {"class": "chain"})


    def get_chain_stream_and_name(self,chain):
        # 上中下游有XXX業的標題，如風力發電、汽電共生、智慧電網、電子商務
        # 其他產業則沒有
        chain_stream_row = chain.find('div', {"class": "chain-title-panel"})
        if chain_stream_row.find('h4'):
            chain_stream = chain_stream_row.find('div').text
            chain_stream_name = chain_stream_row.find('h4').text
        else:
            chain_stream = chain_stream_row.text
            chain_stream_name = None
        return chain_stream, chain_stream_name


    def get_subindustry_names_row(self,chain):
        return chain.find_all('div', {"class": "company-chain-panel"})+\
            chain.find_all('div', {"class": "company-chain-panel2"})+\
            chain.find_all('div', {"class": "company-chain-panel3"})


    def get_chain_groups(self,chain):
        return chain.find_all('div', {"class": "chain-group"})+\
            chain.find_all('div', {"class": "chain-group-2"})


    def get_no_chain_industries(self,industry_chain_overview):
        return industry_chain_overview.find_all('div', {"class": "chain-arrow"})+\
            industry_chain_overview.find_all('div', {"class": "chain-company"})


    def get_all_companies_in_subindustry(self,industry_chain_overview):
        return industry_chain_overview.find_all('div', {"class": "x-hidden"})


    # remove 本國上市公司(6家)的(6家)
    def remove_market_noise(self,row):
        return re.sub(r"\(\d*[\u4e00-\u9fff]*\)", "", row.text)


    def get_company_info(self,company_url_format, company_url):
        company_code = re.sub(f'[{company_url_format}]', "", company_url)
        company_url = self.INDUSTRY_ROOT + company_url
        business_id = self.get_business_id(company_code)
        return company_code, company_url, business_id
        
    def get_business_id(self,code):
        business_id = self.business_id_map.get(code)
        # 若不在business_id_map中，去公開資源找統一編號
        if business_id==None:
            url = self.STCOK_BUSINESS + code
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}
            response = requests.get(url, headers=headers)
            response.encoding = 'utf8'
            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find('table', {"class": "b1 p4_6 r10"}) 
            try:
                business_row = table.find_all('tr')[15] #第16
            except:
                self.business_id_map[code] = None
                return None
            td = business_row.find_all('td')
            if td[0].text=='統一編號':
                self.business_id_map[code] = td[1].text
                return td[1].text
            else:
                self.business_id_map[code] = None
                return None
        else:
            return business_id

    def get_all_company_info(self,industry, Stream, industry_chain_overview):
        companies_in_subindustry = self.get_all_companies_in_subindustry(industry_chain_overview)
        for company_in_subindustry in companies_in_subindustry:
            subindustry_id = company_in_subindustry.get('id').split('_')[-1]
            chain_stream, subindustry_name = self.stat_stream[subindustry_id]

            if company_in_subindustry.find('div', {"class": "subchain-company-list"})==None:
                table = company_in_subindustry.find("table")
                for row in table.find_all("td"):
                    if row.has_attr('rowspan'):
                        row_title = self.remove_market_noise(row)
                        Stream[industry][chain_stream][subindustry_name][row_title] = []
                    else:
                        try:
                            company_name = row.find('a').get('title')
                            company_url = row.find('a').get('href')
                            # 處理白痴前端沒寫好 Trumpf "https://www.trumpf.com/\nhttps://www.trumpf.com/\nhttps://www.trumpf.com/"
                            company_url = company_url.split('\n')[0]
                        except AttributeError:
                            continue
                        else:
                            company_url_format = 'company_basic.php?stk_code='
                            if company_url.startswith(company_url_format):
                                company_code, company_url, business_id = self.get_company_info(company_url_format, company_url)
                            else:
                                company_code = None
                                business_id = None
                            # 處理跳轉問題 "請詳建材營造產業鏈之分布"
                            if not company_name and not company_code:
                                continue
                            company_name = company_name.replace('\n', '')
                            Stream[industry][chain_stream][subindustry_name][row_title].append(
                                                                    (company_name, company_code, business_id, company_url))

            else:
                # 半導體業->IC設計這種點出來還會展開的
                subchain_industries = company_in_subindustry.find_all('div', {"class": "subchain-hover"})+\
                                        company_in_subindustry.find_all('div', {"class": "subchain"})
                del Stream[industry][chain_stream][subindustry_name]

                for subchain_industry in subchain_industries:
                    subchain_industry_title = self.remove_market_noise(subchain_industry)
                    subchain_industry_title = "".join(subchain_industry_title.split())  # remove \xa0
                    subchain_industry_id = subchain_industry.get('id').replace('sc_link_', '')
                    Stream[industry][chain_stream][subchain_industry_title] = defaultdict(dict)

                    subchain_company_prefix = 'sc_company_'
                    table = company_in_subindustry.find('table', {"id": f"{subchain_company_prefix}{subchain_industry_id}"})
                    for row in table.find_all("td"):
                        if row.has_attr('colspan'):
                            row_title = self.remove_market_noise(row)
                            Stream[industry][chain_stream][subchain_industry_title][row_title] = []
                        else:
                            try:
                                company_name = row.find('a').get('title')
                            except AttributeError: 
                                # 國外企業沒上超連結的
                                company_name = "".join(row.text.split())
                                company_code = None
                                company_url = None
                                business_id = None
                            else:
                                company_url = row.find('a').get('href')
                                # 處理白痴前端沒寫好 Trumpf "https://www.trumpf.com/\nhttps://www.trumpf.com/\nhttps://www.trumpf.com/"
                                company_url = company_url.split('\n')[0]
                                company_url_format = 'company_basic.php?stk_code='
                                if company_url.startswith(company_url_format):
                                    company_code, company_url, business_id = self.get_company_info(company_url_format, company_url)
                                else:
                                    company_code = None
                                    business_id = None
                            finally:
                                company_name = company_name.replace('\n', '')
                                Stream[industry][chain_stream][subchain_industry_title][row_title].append(
                                                                    (company_name, company_code, business_id, company_url))
