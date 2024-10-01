# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from bloomingdales.items import BloomingdalesItem
from bloomingdales.db_config import DbConfig
obj = DbConfig()

class BloomingdalesPipeline:
    def process_item(self, item, spider):
        if isinstance(item, BloomingdalesItem):
            obj.insert_data_table(item)

        return item
