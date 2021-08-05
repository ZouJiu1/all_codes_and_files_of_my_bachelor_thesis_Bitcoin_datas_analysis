# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
import numpy as np

class BlockSpider(scrapy.Spider):
    name = 'block'
    start_urls = ['https://blockchain.info/block/0000000000390f816ba4b79cc0e97389c42efc66608d8c102cb8e4dc76f7826a']

    def transform(self,str):
        if ',' in str.split(' ')[0]:
            return float(''.join(str.split(' ')[0].split(',')))
        else:
            return float(str.split(' ')[0])

    def parse(self, response):
        infoDict={}
        SUM = []
        TIME = []
        INPUT = []
        OUTPUT = []
        # if response.status
        for i in response.xpath('//div/div/div[contains(@id,"tx-")]'):
            temi=[]
            temi=i.xpath('.//td/a[contains(@href,"/address")]/text()').extract()
            Temi = []
            for ind in temi:
                if (len(ind) > 36) | (len(ind) < 30):
                    Temi.append(ind)
            for ind in Temi:
                temi.remove(ind)
            if len(temi) == 0:
                continue
            temp =i.xpath('.//span/text()').extract()
            time=temp[0]
            temp.remove(temp[0])
            speciali = specialo = {0: 0}
            lis = []
            for ind in range(len(temp)):
                if 'BTC' in temp[ind]:
                    mark = ind
                    break
            for ind in range(len(temp)):
                if ('(' in temp[ind]) and (')' in temp[ind]):
                    if ind < mark:
                        speciali[1]= temp[ind].split('(')[1].split(')')[0]
                        speciali[0]= 10
                        lis.append(temp[ind])
                    else:
                        specialo[1]= temp[ind].split('(')[1].split(')')[0]
                        specialo[0]= 10
                        lis.append(temp[ind])
                elif (')' in temp[ind]) and ('(' not in temp[ind]):
                    lis.append(temp[ind])
                elif ('(' in temp[ind]) and (')' not in temp[ind]):
                    if ind < mark:
                        speciali[1]= temp[ind].split('(')[1]
                        speciali[0]= 10
                        lis.append(temp[ind])
                    else:
                        specialo[1]= temp[ind].split('(')[1]
                        specialo[0]= 10
                        lis.append(temp[ind])
                else:
                    temp[ind] = self.transform(temp[ind])
            for ind in lis:
                temp.remove(ind)
            if 0 in temp:
                temp.remove(0)
            if len(temp)==0:
                continue
            #             tempo={}
            tempoo = []
            if speciali[0] != 0:  # 有特殊标记的交易
                #                 tempo['special']=speciali[1]+'='+temi[0]
                tempoo.append('special')
                tempoo.append(speciali[1] + '=' + temi[0])
            if specialo[0] != 0:
                #                 tempo['special']=speciali[1]+'='+temi[-1]
                tempoo.append('special')
                tempoo.append(speciali[1] + '=' + temi[-1])
            for ind in range(len(temp) - 1):  # 生成输出地址和对应比特币数量的字典
                #                 tempo[temi[len(temi)-len(temp)+1+ind]]=temp[ind]
                tempoo.append(temi[len(temi) - len(temp) + 1 + ind])
                tempoo.append(temp[ind])
            SUM.append(temp[-1])
            TIME.append(time)
            INPUT.append(temi[:(len(temi) - len(temp)) + 1])
            OUTPUT.append(tempoo)
        infoDict['Sum']=SUM
        infoDict['Time']=TIME
        infoDict['Input']=INPUT
        infoDict['Output']=OUTPUT
            # Block.loc[index, 'Sum'] = temp[-1]
            # Block.loc[index, 'Time'] = time
            # Block.loc[index, 'Input'] = temi[:(len(temi) - len(temp)) + 1]
            # Block.loc[index, 'Output'] = tempoo
        next_h= response.xpath('//div/div/div/table/tr/td/a[contains(@class,"hash-link")]/text()').extract()
        if len(next_h)==4:
            global next_href1
            next_href1 = next_h[3]
        try:
            next_href ='https://blockchain.info/block/'+next_h[2]
        except:
            print('有分支出现，需要进行筛选！')
            next_href='https://blockchain.info/block/'+next_href1
            print(next_href)
            yield scrapy.Request(next_href, callback=self.parse)
        import time
        time.sleep(0.1)
        yield infoDict
        if next_href!='https://blockchain.info/block/000000000000000082ccf8f1557c5d40b21edabb18d2d691cfbf87118bac7254':
            H = 0
            while H!=100:
                try:
                    yield scrapy.Request(next_href, callback=self.parse)
                    H=100
                except:
                    import time
                    time.sleep(3)
                    H=0

