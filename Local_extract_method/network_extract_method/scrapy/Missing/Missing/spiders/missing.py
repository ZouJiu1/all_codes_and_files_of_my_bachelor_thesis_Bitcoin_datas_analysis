# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
import numpy as np

class MissingSpider(scrapy.Spider):
    name = 'missing'
    addr = pd.read_csv(r'E:\scrapy\Missing\missing.csv', encoding="utf-8")
    start_urls = ['https://blockchain.info/block/' + addr.loc[len(addr) - 1, 'Addr']]

    def transform(self, str):
        if ',' in str.split(' ')[0]:
            return float(''.join(str.split(' ')[0].split(',')))
        else:
            return float(str.split(' ')[0])

    def parse(self, response):
        infoDict = {}
        SUM = []
        for i in response.xpath('//div/div/div[contains(@id,"tx-")]'):
            temi = []
            temi = i.xpath('.//td/a[contains(@href,"/address")]/text()').extract()
            Temi = []
            for ind in temi:
                if (len(ind) > 36) | (len(ind) < 30):
                    Temi.append(ind)
            for ind in Temi:
                temi.remove(ind)
            if len(temi) == 0:
                continue
            temp = i.xpath('.//span/text()').extract()
            temp.remove(temp[0])
            lis = []
            for ind in range(len(temp)):
                if 'BTC' in temp[ind]:
                    mark = ind
                    break
            for ind in range(len(temp)):
                if ('(' in temp[ind]) and (')' in temp[ind]):
                    if ind < mark:
                        lis.append(temp[ind])
                    else:
                        lis.append(temp[ind])
                elif (')' in temp[ind]) and ('(' not in temp[ind]):
                    lis.append(temp[ind])
                elif ('(' in temp[ind]) and (')' not in temp[ind]):
                    if ind < mark:
                        lis.append(temp[ind])
                    else:
                        lis.append(temp[ind])
            for ind in lis:
                temp.remove(ind)
            if 0 in temp:
                temp.remove(0)
            if len(temp) == 0:
                continue
            SUM.append(temp[-1])
        ADDR = ['a'] * (len(SUM) - 1)
        next_h = response.xpath('//div/div/div/table/tr/td/a[contains(@class,"hash-link")]/text()').extract()
        if len(next_h) == 4:
            global next_href1
            next_href1 = next_h[3]
        try:
            next_href = 'https://blockchain.info/block/' + next_h[2]
        except:
            print('有分支出现，需要进行筛选！')
            next_href = 'https://blockchain.info/block/' + next_href1
            print(next_href)
            yield scrapy.Request(next_href, callback=self.parse)
        import time
        time.sleep(0.01)
        ADDR.append(next_href.split('/')[-1])
        infoDict['Addr'] = ADDR
        infoDict['Height'] = [int(
            response.xpath('//div/div/div/table/tr/td/a[contains(@href,"block-height")]/text()').extract()[0])] * len(
            SUM)
        yield infoDict
        if next_href != 'https://blockchain.info/block/0000000000000085ea727ee53848d3c57e940af78d1116b33f315f244b5d9e04':
            H = 0
            while H != 100:
                try:
                    yield scrapy.Request(next_href, callback=self.parse)
                    H = 100
                except:
                    import time
                    time.sleep(3)
                    H = 0
