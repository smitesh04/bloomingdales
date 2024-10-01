import json
import time
from typing import Iterable

import requests
from parsel import Selector

from bloomingdales.items import BloomingdalesItem
import datetime
import re
import os
import scrapy
from scrapy import Request
from scrapy.cmdline import execute as ex
from bloomingdales.db_config import DbConfig
from bloomingdales.common_func import page_write, create_md5_hash
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
today_date = datetime.datetime.today().strftime('%d_%m_%Y')

obj.cur.execute(f'select * from {obj.links_pdp_table} where status=2')
rows = obj.cur.fetchall()
for row in rows:
    link = row['link']
    # link = 'https://www.bloomingdales.com/shop/product/frame-duo-fold-crew-shirt?ID=5011694&CategoryID=11536'
    hashid = create_md5_hash(link)
    pagesave_dir = rf"C:/Users/Actowiz/Desktop/pagesave/bloomingdales/{today_date}"
    file_name = fr"{pagesave_dir}/{hashid}.html"
    product_id = row['product_id']

    if os.path.exists(file_name):
        file = open(file_name, 'r', encoding= 'utf8')
        response1_text = file.read()

        file.close()
    else:
        response1 = requests.get(url= link, headers= headers())
        response1_text = response1.text

    selector1 = Selector(response1_text)

    sizes_list = ''
    color_final = ''

    if not os.path.exists(file_name):
        page_write(pagesave_dir, file_name, response1_text)
    product_div = selector1.xpath("//div[@class='product-view']")
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
                sizes_list = product_div.xpath(
                    ".//select[@aria-label='Size']/option[not(contains(@value,'select-a-size'))]/text()").getall()
                sizes_list = ', '.join(sizes_list)
            if not sizes_list:
                sizes_list = product_div.xpath(
                    ".//span[contains(@class, 'label') and contains(text(), 'Size')]/following-sibling::span/text()").get()
                sizes_list = sizes_list.split(';')
                sizes_list = sizes_list[0]

            if not sizes_list:
                for i in range(1, 1000):
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
                color_list = product_div.xpath(
                    ".//span[contains(@class, 'label') and contains(text(), 'Color')]/following-sibling::span/text()").get()
                if '/' in color_list:
                    color_list = color_list.split('/')
                color_final = ', '.join(color_list)
            if not color_list:
                for i in range(1, 1000):
                    print('color script update')
                    time.sleep(i)

    try:
        review_count_ = re.findall('"reviewCount":.*?,', response1_text)[0]
        review_count = review_count_.replace('"reviewCount":"', '').replace('",', '').replace(',', '')
        review_count = int(review_count)
    except:
        review_count = 0
    category_list = selector1.xpath("//li[@class='p-menuitem']//span/text()").getall()
    category = '-'.join(category_list)
    title = product_div.xpath(".//span[@itemprop='name']/text()").get()
    price = product_div.xpath(".//div[@data-el='price-details']//span/text()").get()
    color_list = color_final
    rating = product_div.xpath(".//span[contains(@class,'rating-average')]/text()").get()
    description = product_div.xpath(
        ".//div[@data-analytics-key='product_details']/following-sibling::div/div[@class='p-accordion-content']//text()").getall()

    product_description = ' | '.join(description)
    product_description = product_description.strip()
    material_care_list = product_div.xpath(
        ".//div[@data-analytics-key='materials_and_care']/following-sibling::div//span/text()").getall()
    material_and_care = ' | '.join(material_care_list)

    review_url = f"https://www.bloomingdales.com/xapi/digital/v1/product/{product_id}/reviews?sort=NEWEST&limit=8&offset=0"
    hashid_review = create_md5_hash(review_url)
    pagesave_dir_review = rf"C:/Users/Actowiz/Desktop/pagesave/bloomingdales/{today_date}"
    file_name_review = fr"{pagesave_dir_review}/{hashid_review}.json"

    if os.path.exists(file_name_review):
        file = open(file_name_review, 'r', encoding= 'utf8')
        response2_text = file.read()
        file.close()

    else:
        if int(review_count) > 0:
            response2 = requests.get(url= review_url, headers= headers())
            response2_text = response2.text
    if not os.path.exists(file_name_review):
        page_write(pagesave_dir_review, file_name_review, response2_text)

    if int(review_count) > 8:
        for offset in range(38, review_count, 30):
            review_url = f"https://www.bloomingdales.com/xapi/digital/v1/product/{product_id}/reviews?sort=NEWEST&limit=30&offset={offset}"
            hashid_review = create_md5_hash(review_url)
            pagesave_dir_review = rf"C:/Users/Actowiz/Desktop/pagesave/bloomingdales/{today_date}"
            file_name_review = fr"{pagesave_dir_review}/{hashid_review}.json"

            if os.path.exists(file_name_review):
                file = open(file_name_review, 'r', encoding= 'utf8')
                response3_text = file.read()
                file.close()
            else:
                response3 = requests.get(url= review_url, headers= headers())
                response3_text = response3.text
    elif review_count > 0:
        file = open(file_name_review, 'r', encoding='utf8')
        response3_text = file.read()
        file.close()
    else:
        file = open(file_name, 'r', encoding='utf8')
        response3_text = file.read()
        file.close()
    if not os.path.exists(file_name_review):
        page_write(pagesave_dir_review, file_name_review, response3_text)

    item = {}

    item['category'] = category
    item['scrapeDate'] = datetime.datetime.today().strftime("%d-%m-%Y")
    item['url'] = link
    item['title'] = title
    item['price'] = price
    item['discountedPrice'] = ''
    item['color_list'] = color_list
    item['size_list'] = sizes_list
    item['reviewCount'] = review_count
    item['rating'] = rating
    item['product_description'] = product_description
    item['material_care_instruction'] = material_care_list
    item['source_country'] = 'US'
    item['age'] = ''
    item['body_type'] = ''
    item['height'] = ''
    item['size_purchased'] = ''
    item['fits'] = ''
    item['weight'] = ''
    item['pagesave_pdp'] = file_name
    item['pagesave_review'] = file_name_review

    if review_count > 0:
        response_ = response3_text
        response_ = response_.replace('\\"', '')
        jsn_review = json.loads(response_)
        for review in jsn_review['review']['reviews']:
            review_user = review['userNickname']
            review_user_rating = review['rating']
            review_title = review['title']
            review_text = review['reviewText']
            review_date = review['submissionTime']
            review_date_strp = datetime.datetime.strptime(review_date, "%b %d, %Y")
            review_date_strf = review_date_strp.strftime("%d-%m-%Y")

            item['review_date'] = review_date_strf
            item['review_title'] = review_title
            item['review_text'] = review_text
            item['individual_review_rating'] = review_user_rating
            hashid_review = create_md5_hash(f"{item['url']}{item['review_title']}{item['review_date']}")
            item['hashid_review'] = hashid_review

            obj.insert_data_table(item)


    else:

        item['review_date'] = ''
        item['review_title'] = ''
        item['review_text'] = ''
        item['individual_review_rating'] = ''
        hashid_review = create_md5_hash(f"{item['url']}{item['review_title']}{item['review_date']}")
        item['hashid_review'] = hashid_review

        obj.insert_data_table(item)











