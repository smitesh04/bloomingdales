from typing import Iterable
import os
import scrapy
from scrapy import Request
from scrapy.cmdline import execute as ex
import re
from bloomingdales.db_config import DbConfig
obj = DbConfig()

class LinksSpider(scrapy.Spider):
    name = "links"
    # allowed_domains = ["."]
    # start_urls = ["https://."]
    def start_requests(self):
        path = r'C:/Users/Actowiz/Desktop/Smitesh_Docs/Project/bloomingdales/sitemap/extracted'
        files = os.listdir(path)
        for file in files:
            if 'pdp' in file:
                yield scrapy.Request(
                    url= 'file:///'+path+'/'+file,
                    callback= self.parse
                )

    def parse(self, response):
        all_urls = re.findall('<loc>.*?</loc>', response.text)
        for url in all_urls:
            url = url.replace('<loc>', '').replace('</loc>', '')
            obj.insert_links_pdp_table(url)

if __name__ == '__main__':

    ex("scrapy crawl links".split())