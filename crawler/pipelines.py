# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import mariadb
import sys
import time

class CrawlerPipeline:
    def process_item(self, item, spider):

        if item['chain_group'] == {}:
            item['chain_group'] = None

        if item['stream_name'] == {}:
            item['stream_name'] = None 

        return item

class MariaDBPipeline:
    def __init__(self):
        try:
            self.connect = mariadb.connect(host='localhost', 
                                            user='root',
                                            password='root',)
        except mariadb.Error as e:
            spider.logger.error(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)
        self.cur = self.connect.cursor()
        self.DATABASE = 'industry_chain'
        self.TABLE ='company_info'

        try:
            self.cur.execute(f"DROP DATABASE {self.DATABASE}") 
        except: 
            pass
        finally:    
            self.cur.execute(f"CREATE DATABASE {self.DATABASE}") 

        self.cur.execute(f"USE {self.DATABASE}") 
        self.cur.execute(f"""CREATE TABLE {self.TABLE} (
                            id INT AUTO_INCREMENT,
                            industry CHAR(255), 
                            industry_id CHAR(255), 
                            stream CHAR(255), 
                            stream_name CHAR(255), 
                            industry_module CHAR(255), 
                            chain_group CHAR(255), 
                            market CHAR(255), 
                            name CHAR(255), 
                            code CHAR(255), 
                            business_uid CHAR(255), 
                            url CHAR(255),
                            update_time TIMESTAMP NOT NULL DEFAULT current_timestamp() ON UPDATE CURRENT_TIMESTAMP,
                            refresh_time TIMESTAMP NOT NULL DEFAULT current_timestamp(),
                            create_time TIMESTAMP NOT NULL DEFAULT current_timestamp(),
                            PRIMARY KEY (id))
                    """) 

    def process_item(self, item, spider):
        try:
            statement = f"""
                        INSERT INTO {self.TABLE}
                        (industry, industry_id, stream, stream_name, industry_module, chain_group, market, name, code, business_uid, url) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """

            data = (item['industry'], 
                    item['industry_id'], 
                    item['stream'], 
                    item['stream_name'], 
                    item['industry_module'], 
                    item['chain_group'], 
                    item['market'], 
                    item['company_name'], 
                    item['company_code'], 
                    item['business_id'], 
                    item['company_url'])
            self.cur.execute(statement, data)
            self.connect.commit()
        except mariadb.Error as e:
            spider.logger.error(f"Error adding entry to database: {e}")
        return item

    def close_spider(self, spider):
        # 資料處理後關閉資料庫
        self.connect.close()  # 將此位址關閉
