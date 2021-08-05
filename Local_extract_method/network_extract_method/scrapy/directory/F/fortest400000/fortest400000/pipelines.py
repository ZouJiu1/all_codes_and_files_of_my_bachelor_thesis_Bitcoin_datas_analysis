# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pandas as pd
import numpy as np
class Fortest400000Pipeline(object):
    def process_item(self, item, spider):
        return item
class Fortest400000infoPipeline(object):
    def open_spider(self,spider):
        # self.f=open('blockchain.txt','w')
        self.h = pd.read_csv(r'F:\fortest400000\block400000.csv',encoding = "utf-8")
    def close_spider(self,spider):
        # self.f.close()
        self.h.to_csv(r'F:\fortest400000\block400000.csv',index=False,encoding = "utf-8")
    def process_item(self,item,spider):
        try:
            # line=str(dict(item))
            # self.f.write(line)
            # self.f.write('\n\n')
            # index=0

            self.f = pd.DataFrame(item)
            self.h = pd.concat([self.h, self.f])
        except:
            pass
        return item