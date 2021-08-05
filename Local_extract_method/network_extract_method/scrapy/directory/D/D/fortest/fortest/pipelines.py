# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pandas as pd
import numpy as np

class FortestPipeline(object):
    def process_item(self, item, spider):
        return item
class FortestinfoPipeline(object):
    def open_spider(self,spider):
        # self.f=open('blockchain.txt','w')
        self.h = pd.read_csv(r'D:\fortest\block.csv',encoding = "utf-8")
    def close_spider(self,spider):
        # self.f.close()
        self.h.to_csv(r'D:\fortest\block.csv',index=False,encoding = "utf-8")
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