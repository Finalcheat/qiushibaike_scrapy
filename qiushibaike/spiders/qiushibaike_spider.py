#!/usr/bin/env python
# -*- coding: utf-8 -*-

import scrapy
import re
import pymysql
from qiushibaike.items import QiushibaikeItem
from qiushibaike.private_settings import MYSQL_CONFIG


class QiushibaikeSpider(scrapy.Spider):
    """
    糗事百科spider
    """

    name = "qiushibaike"
    allowed_domains = ["qiushibaike.com"]
    start_urls = [
        "http://www.qiushibaike.com/text/",
    ]

    conn = pymysql.connect(host=MYSQL_CONFIG["host"], port=MYSQL_CONFIG["port"], user=MYSQL_CONFIG["user"],
                           password=MYSQL_CONFIG["password"], db=MYSQL_CONFIG["database"], charset=MYSQL_CONFIG["charset"],
                           cursorclass=pymysql.cursors.DictCursor)

    CREATE_TABLE_SQL = """CREATE TABLE IF NOT EXISTS `joke_source` (
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `content` text NOT NULL COMMENT '段子内容',
        `md5` varchar(128) NOT NULL COMMENT '段子内容md5值',
        `pubtime` int(11) NOT NULL COMMENT '段子在源网站的发布时间戳',
        `crawl_time` int(11) NOT NULL COMMENT '抓取时间时间戳',
        `like` int(11) NOT NULL DEFAULT '0' COMMENT '点赞数',
        `dislike` int(11) NOT NULL DEFAULT '0' COMMENT '点踩数',
        `href` varchar(128) NOT NULL COMMENT '源链接',
        `source` varchar(32) NOT NULL COMMENT '来源',
        PRIMARY KEY (`id`),
        UNIQUE KEY `MD5_INDEX` (`md5`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""


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
