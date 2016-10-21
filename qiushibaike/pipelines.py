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
