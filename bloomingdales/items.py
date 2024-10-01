# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BloomingdalesItem(scrapy.Item):
    # define the fields for your item here like:
    category = scrapy.Field()
    scrapeDate = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    discountedPrice = scrapy.Field()
    color_list = scrapy.Field()
    size_list = scrapy.Field()
    reviewCount = scrapy.Field()
    rating = scrapy.Field()
    product_description = scrapy.Field()
    material_care_instruction = scrapy.Field()
    review_date = scrapy.Field()
    review_title = scrapy.Field()
    review_text = scrapy.Field()
    individual_review_rating = scrapy.Field()
    age = scrapy.Field()
    body_type = scrapy.Field()
    height = scrapy.Field()
    size_purchased = scrapy.Field()
    fits = scrapy.Field()
    weight = scrapy.Field()
    source_country = scrapy.Field()
    hashid_review = scrapy.Field()
    pagesave_pdp = scrapy.Field()
    pagesave_review = scrapy.Field()

