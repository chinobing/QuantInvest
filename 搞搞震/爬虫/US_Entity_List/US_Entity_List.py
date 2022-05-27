#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2021-12-17 13:54:24
# Project: US_Entity_List

from pyspider.libs.base_handler import *
import requests

class Handler(BaseHandler):
    crawl_config = {
    }

    @every(minutes= 10 * 60)
    def on_start(self):
        self.crawl('https://www.commerce.gov/tags/entity-list', callback=self.index_page)

    @config(age=1 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('.view-content a[href^="http"]').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    @config(priority=2)
    def detail_page(self, response):
        matches=['tags','issues']
        r_url = response.url
        text = response.doc('title').text()
        if not any(x in r_url for x in matches):
            ##### 这类使用了Bark作为推送，可以根据实际情况使用server酱、钉钉、telegramBot等形式作为推送######
            #title = '美国实体清单'
            #groupName = 'US_Entity_List'
            #msg = '{}'.format(text)
            #url = 'http://YOUR IP or DOMAIN/APITOKEN/{}/{}?url={}&group={}'.format(title, msg, r_url, groupName)
            #response = requests.request("POST", url)
            ###########################################################################################
            return {
                "url": r_url,
                "title": text,
            }
