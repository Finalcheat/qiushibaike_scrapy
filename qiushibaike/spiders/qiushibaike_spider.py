#!/usr/bin/env python
# -*- coding: utf-8 -*-

import scrapy
import re
from qiushibaike.items import QiushibaikeItem


class QiushibaikeSpider(scrapy.Spider):
    """
    糗事百科spider
    """

    name = "qiushibaike"
    allowed_domains = ["qiushibaike.com"]
    start_urls = [
        "http://www.qiushibaike.com/text/",
    ]

    def parse(self, response):
        articles = response.css('div.article')
        for article in articles:
            content = article.xpath('.//div[@class="content"]').extract()[0]
            content = content.replace('<div class="content">', '<div>')
            content = content.replace("\n", "")
            like = article.xpath('.//div[@class="stats"]/span[@class="stats-vote"]/i[@class="number"]/text()').extract()[0]
            like = int(like)
            dislike = 0
            href = article.xpath('.//a[@class="contentHerf"]/@href').extract()[0]
            if not href.startswith('http'):
                href = 'http://www.qiushibaike.com' + href
            source = u"糗事百科"

            item = QiushibaikeItem(content=content, like=like, dislike=dislike, href=href, source=source)
            yield item
