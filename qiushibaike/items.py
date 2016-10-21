# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class QiushibaikeItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
    content = scrapy.Field()
    pubtime = scrapy.Field()
    like = scrapy.Field()
    dislike = scrapy.Field()
    href = scrapy.Field()
    source = scrapy.Field()
