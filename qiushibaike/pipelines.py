# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import pymysql
import hashlib
import time
from qiushibaike.private_settings import MYSQL_CONFIG


class QiushibaikePipeline(object):

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

    conn = pymysql.connect(host=MYSQL_CONFIG["host"], port=MYSQL_CONFIG["port"], user=MYSQL_CONFIG["user"],
                           password=MYSQL_CONFIG["password"], db=MYSQL_CONFIG["database"], charset=MYSQL_CONFIG["charset"],
                           cursorclass=pymysql.cursors.DictCursor)

    def find_dup(self, item):
        content = item["content"]
        hash_md5 = hashlib.md5(content)
        md5 = hash_md5.hexdigest()
        with self.conn:
            with self.conn.cursor() as cur:
                sql = "SELECT md5 FROM `joke_source` WHERE `md5` = %s"
                cur.execute(sql, (md5,))
                rows = cur.fetchall()
                if rows:
                    return None
        return md5

    def process_item(self, item, spider):
        content = item["content"]
        md5 = self.find_dup(item)
        if md5 is not None:
            pubtime = int(time.time())
            crawl_time = int(time.time())
            with self.conn:
                with self.conn.cursor() as cur:
                    sql = """INSERT INTO `joke_source` (`content`, `md5`, `pubtime`, `crawl_time`, `like`, `dislike`, `href`, `source`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                    cur.execute(sql, (content, md5, pubtime, crawl_time, item["like"], item["dislike"], item["href"],
                                    item["source"]))
                    self.conn.commit()

        return item
