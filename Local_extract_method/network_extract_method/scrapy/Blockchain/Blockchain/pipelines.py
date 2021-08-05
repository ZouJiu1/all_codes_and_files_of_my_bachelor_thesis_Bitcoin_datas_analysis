# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pandas as pd
import numpy as np
class BlockchainPipeline(object):
    def process_item(self, item, spider):
        return item
class BlockchaininfoPipeline(object):
    def open_spider(self,spider):
        # self.f=open('blockchain.txt','w')
        self.h = pd.read_csv(r'E:\scrapy\Blockchain\block.csv')
    def close_spider(self,spider):
        # self.f.close()
        self.h.to_csv(r'E:\scrapy\Blockchain\block.csv',index=False)
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