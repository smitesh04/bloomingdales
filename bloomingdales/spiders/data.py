import json
import random
import sys
import time
from typing import Iterable
from bloomingdales.items import BloomingdalesItem
import datetime
import re
import os
import scrapy
from scrapy import Request
from scrapy.cmdline import execute as ex
from bloomingdales.db_config import DbConfig
from bloomingdales.common_func import page_write, create_md5_hash, storm, scraper, zyte
from fake_useragent import UserAgent
ua = UserAgent()

obj = DbConfig()

def headers():
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'user-agent': ua.random,
    }
    return headers
def headers_review():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=1, i',
        'user-agent': ua.random,
    }

    return headers
today_date = datetime.datetime.today().strftime('%d_%m_%Y')

class DataSpider(scrapy.Spider):
    name = "data"
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def start_requests(self):
        # obj.cur.execute(f'select * from {obj.links_pdp_table} where status=0 limit {self.start},{self.end}')
        obj.cur.execute(f'select * from {obj.links_pdp_table} where status=0 and id >= {self.start} and id <= {self.end}')
        rows = obj.cur.fetchall()
        for row in rows:
            link = row['link']
            hashid = create_md5_hash(link)
            pagesave_dir = rf"C:/Users/Actowiz/Desktop/pagesave/bloomingdales/{today_date}"
            file_name = fr"{pagesave_dir}/{hashid}.html"
            meta = {}
            meta['file_name'] = file_name
            meta['pagesave_dir'] = pagesave_dir
            meta['hashid'] = hashid
            meta['link'] = link
            meta['product_id'] = row['product_id']

            if os.path.exists(file_name):
                yield scrapy.Request(
                    url='file:///' + file_name,
                    cb_kwargs=meta,
                    callback= self.parse,
                    dont_filter= True
                )
            else:
                yield scrapy.Request(url= link, headers= headers(), cb_kwargs= meta, callback= self.parse, dont_filter= True)

    def parse(self, response, **kwargs):
        sizes_list = ''
        color_final = ''
        product_id = kwargs['product_id']
        file_name = kwargs['file_name']
        pagesave_dir = kwargs['pagesave_dir']
        if not os.path.exists(file_name):
            page_write(pagesave_dir, file_name, response.text)
        product_div = response.xpath("//div[@class='product-view']")
        labels_list = product_div.xpath(".//span[contains(@class, ' label')]/text()").getall()
        if not labels_list:
            labels_list = product_div.xpath(".//span[contains(@class, 'label')]/text()").getall()
        for label in labels_list:
            label = label.replace(':', '')
            if 'size' in label.lower():
                sizes_list = product_div.xpath(".//button[contains(@class, 'size-tile')]/span/text()").getall()
                if sizes_list:
                    sizes_list = ', '.join(sizes_list)
                if not sizes_list:
                    sizes_list = product_div.xpath(".//select[@aria-label='Size']/option[not(contains(@value,'select-a-size'))]/text()").getall()
                    sizes_list = ', '.join(sizes_list)
                if not sizes_list:
                    sizes_list = product_div.xpath(".//span[contains(@class, 'label') and contains(text(), 'Size')]/following-sibling::span/text()").get()
                    try:
                        sizes_list = sizes_list.split(';')
                        sizes_list = sizes_list[0]
                    except:pass
                if not sizes_list:
                    sizes_list = product_div.xpath(".//span[contains(@class, 'label') and contains(text(), 'Size')]//following-sibling::div//option/@value").getall()
                    sizes_list = ', '.join(sizes_list)
                if not sizes_list:
                    for i in range(1,1000):
                        print('sizes script update')
                        time.sleep(i)

            if 'color' in label.lower():
                color_label_list = product_div.xpath(".//input[contains(@aria-label, 'Color:')]/@aria-label").getall()
                color_list = list()
                for color_label in color_label_list:
                    color = color_label.replace('Color: ', '')
                    if color not in color_list:
                        color_list.append(color)
                    color_final = ', '.join(color_list)
                if not color_list:
                    color_list = product_div.xpath(".//span[contains(@class, 'label') and contains(text(), 'Color')]/following-sibling::span/text()").get()
                    color_final = color_list
                if not color_list:
                    for i in range(1,1000):
                        print('color script update')
                        time.sleep(i)
        try:
            review_count_ = re.findall('"reviewCount":.*?,', response.text)[0]
            review_count = review_count_.replace('"reviewCount":"', '').replace('",', '').replace(',', '')
            kwargs['review_count'] = int(review_count)
            print(f'review count is {review_count} for {kwargs["link"]}')
        except:
            review_count = 0
            kwargs['review_count'] = 0
        category_list = response.xpath("//li[@class='p-menuitem']//span/text()").getall()
        category = '-'.join(category_list)
        kwargs['category'] = category
        title = product_div.xpath(".//span[@itemprop='name']/text()").get()
        kwargs['title'] = title
        price = product_div.xpath(".//div[@data-el='price-details']//span/text()").get()
        kwargs['price'] = price
        color_list = color_final
        kwargs['color_list'] = color_final
        kwargs['size_list'] = sizes_list
        rating = product_div.xpath(".//span[contains(@class,'rating-average')]/text()").get()
        kwargs['rating'] = rating
        description = product_div.xpath(".//div[@data-analytics-key='product_details']/following-sibling::div/div[@class='p-accordion-content']//text()").getall()
        product_description = ' | '.join(description)
        kwargs['product_description'] = product_description
        material_care_list = product_div.xpath(".//div[@data-analytics-key='materials_and_care']/following-sibling::div//span/text()").getall()
        material_and_care = ' | '.join(material_care_list)
        kwargs['material_care_list'] = material_and_care
        review_url = f"https://www.bloomingdales.com/xapi/digital/v1/product/{product_id}/reviews?sort=NEWEST&limit=8&offset=0"
        hashid_review = create_md5_hash(review_url)
        pagesave_dir_review = rf"C:/Users/Actowiz/Desktop/pagesave/bloomingdales/{today_date}"
        file_name_review = fr"{pagesave_dir_review}/{hashid_review}.json"
        kwargs['pagesave_dir_review'] = pagesave_dir_review
        kwargs['file_name_review'] = file_name_review
        kwargs['hashid_review'] = hashid_review
        kwargs['proxy'] = scraper()
        if os.path.exists(file_name_review):
            yield scrapy.Request(url= 'file:///' + file_name_review, cb_kwargs= kwargs, callback= self.parse_review, dont_filter= True)
        else:
            if int(review_count) > 0:
                yield scrapy.Request(url= review_url, headers= headers_review(), cb_kwargs=kwargs, meta={'http':scraper()}, callback= self.parse_review, dont_filter= True)
            else:
                yield scrapy.Request(url= 'file:///' + file_name, cb_kwargs= kwargs, callback= self.parse_review, dont_filter= True)

    def parse_review(self, response, **kwargs):
        pagesave_dir_review = kwargs['pagesave_dir_review']
        file_name_review = kwargs['file_name_review']
        if not os.path.exists(file_name_review):
            page_write(pagesave_dir_review, file_name_review, response.text)
        review_count = kwargs['review_count']
        if review_count > 8:
            item = BloomingdalesItem()
            item['category'] = kwargs['category']
            item['scrapeDate'] = datetime.datetime.today().strftime("%d-%m-%Y")
            item['url'] = kwargs['link']
            item['title'] = kwargs['title']
            item['price'] = kwargs['price']
            item['discountedPrice'] = ''
            item['color_list'] = kwargs['color_list']
            item['size_list'] = kwargs['size_list']
            item['reviewCount'] = kwargs['review_count']
            item['rating'] = kwargs['rating']
            item['product_description'] = kwargs['product_description']
            item['material_care_instruction'] = kwargs['material_care_list']
            item['source_country'] = 'US'
            item['age'] = ''
            item['body_type'] = ''
            item['height'] = ''
            item['size_purchased'] = ''
            item['fits'] = ''
            item['weight'] = ''
            item['pagesave_pdp'] = kwargs['file_name']
            item['pagesave_review'] = file_name_review
            response_ = response.text
            response_ = response_.replace('\\"', '')
            jsn_review = json.loads(response_)
            for review in jsn_review['review']['reviews']:
                review_user_rating = review['rating']
                try:review_title = review['title']
                except:review_title=''
                try:review_text = review['reviewText']
                except:review_text = ''
                review_date = review['submissionTime']
                review_date_strp = datetime.datetime.strptime(review_date, "%b %d, %Y")
                review_date_strf = review_date_strp.strftime("%d-%m-%Y")

                item['review_date'] = review_date_strf
                item['review_title'] = review_title
                item['review_text'] = review_text
                item['individual_review_rating'] = review_user_rating
                hashid_review = create_md5_hash(f"{item['url']}{item['review_title']}{item['review_date']}{item['review_text']}")
                item['hashid_review'] = hashid_review

                yield item

            for offset in range(8, review_count, 30):
                review_url = f"https://www.bloomingdales.com/xapi/digital/v1/product/{kwargs['product_id']}/reviews?sort=NEWEST&limit=30&offset={offset}"
                hashid_review = create_md5_hash(review_url)
                pagesave_dir_review = rf"C:/Users/Actowiz/Desktop/pagesave/bloomingdales/{today_date}"
                file_name_review = fr"{pagesave_dir_review}/{hashid_review}.json"
                kwargs['file_name_review'] = file_name_review
                kwargs['pagesave_dir_review'] = pagesave_dir_review
                kwargs['hashid_review'] = hashid_review

                if os.path.exists(file_name_review):
                    yield scrapy.Request(url= 'file:///' + file_name_review, cb_kwargs= kwargs, callback= self.parse_final, dont_filter= True)
                else:
                    yield scrapy.Request(url= review_url, headers= headers_review(), cb_kwargs= kwargs, meta={'http':scraper()}, callback= self.parse_final, dont_filter= True)
        elif review_count > 0:
            yield scrapy.Request(url='file:///' + kwargs['file_name_review'], cb_kwargs=kwargs, callback=self.parse_final,
                                 dont_filter=True)
        else:
            yield scrapy.Request(url= 'file:///' + kwargs['file_name'], cb_kwargs= kwargs, callback= self.parse_final, dont_filter= True)

    def parse_final(self, response, **kwargs):
        review_count = kwargs['review_count']
        pagesave_dir_review = kwargs['pagesave_dir_review']
        file_name_review = kwargs['file_name_review']
        kwargs['file_name_review'] = file_name_review
        kwargs['pagesave_dir_review'] = pagesave_dir_review
        if not os.path.exists(file_name_review):
            page_write(pagesave_dir_review, file_name_review, response.text)

        item = BloomingdalesItem()
        item['category'] = kwargs['category']
        item['scrapeDate'] = datetime.datetime.today().strftime("%d-%m-%Y")
        item['url'] = kwargs['link']
        item['title'] = kwargs['title']
        item['price'] = kwargs['price']
        item['discountedPrice'] = ''
        item['color_list'] = kwargs['color_list']
        item['size_list'] = kwargs['size_list']
        item['reviewCount'] = kwargs['review_count']
        item['rating'] = kwargs['rating']
        item['product_description'] = kwargs['product_description']
        item['material_care_instruction'] = kwargs['material_care_list']
        item['source_country'] = 'US'
        item['age'] = ''
        item['body_type'] = ''
        item['height'] = ''
        item['size_purchased'] = ''
        item['fits'] = ''
        item['weight'] = ''
        item['pagesave_pdp'] = kwargs['file_name']
        item['pagesave_review'] = file_name_review

        if review_count > 0:
            response_ = response.text
            response_ = response_.replace('\\"', '')
            jsn_review = json.loads(response_)
            for review in jsn_review['review']['reviews']:
                # review_user = review['userNickname']
                review_user_rating = review['rating']
                review_date = review['submissionTime']
                review_date_strp = datetime.datetime.strptime(review_date, "%b %d, %Y")
                review_date_strf = review_date_strp.strftime("%d-%m-%Y")
                item['review_date'] = review_date_strf
                try:item['review_title'] = review['title']
                except:item['review_title'] = ''
                try:item['review_text'] = review['reviewText']
                except:item['review_text'] = ''
                item['individual_review_rating'] = review_user_rating
                hashid_review = create_md5_hash(f"{item['url']}{item['review_title']}{item['review_date']}")
                item['hashid_review'] = hashid_review

                yield item
        else:
            item['review_date'] = ''
            item['review_title'] = ''
            item['review_text'] = ''
            item['individual_review_rating'] = ''
            hashid_review = create_md5_hash(f"{item['url']}{item['review_title']}{item['review_date']}")
            item['hashid_review'] = hashid_review

            yield item

        obj.update_links_pdp_status(item['url'])

if __name__ == '__main__':
    try:
        start = sys.argv[1]
        end = sys.argv[2]
    except:
        start = 0
        end = 100

    ex(f"scrapy crawl data -a start={start} -a end={end}".split())