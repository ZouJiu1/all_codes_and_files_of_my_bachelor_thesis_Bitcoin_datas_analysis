# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import FormRequest
import pandas as pd
import numpy as np
import os

class SampleSpider(scrapy.Spider):
    name = 'sample'
    def start_requests(self):
        self.start =502001
        self.end =504001
        df=pd.DataFrame(columns=['Height','Input','Output','Sum','Time'])
        df.to_csv(r'e:\extract\%s-%s.csv' % (self.start,self.end-1),index=False)
        x = 'file:///E:/Data/500000-507999/save/'
        urls=[]
        for i in range(self.start,self.end,1):
            urls.append(x+str(i)+'.html')
        for url in urls:
            yield scrapy.Request(url=url,callback=self.parse)

    def loc(self,list):
        for j in range(len(list)):  # 找到输入地址位置，先找第一个数字
            if not (('.' not in list[j]) and (not list[j].isdigit())):
                return j
    def parse(self, response):
        infoDict = {}
        SUM = []
        TIME = []
        INPUT = []
        OUTPUT = []
        for i in response.xpath('//div/div/div[contains(@id,"tx-")]'):
            TEXT=i.xpath('string(.)').extract()
            if 'No Inputs' in TEXT[0]:            #删除生成比特币的数据
                continue
            text=TEXT[0].replace(' BTC',' ')      #删去比特币符号
            text=text[(text.index(' ')+1):]           #提取有用信息
            Time=text[0:19]
            text=text[19:]
            w=i.xpath('.//td/a[contains(@href,"/address")]/text()').extract()
            for addr in w:                   #将地址增加空格，方便处理分离
                text=text.replace(addr,addr+' ')
            # print('\n\n\n',text)
            while '(' in text:         #删除额外信息
                index_s = text.index('(')
                if '(' in text[(index_s + 1):(index_s +30)]:
                    index_ss = text[(index_s + 1):].index('(')
                    index_ee = text[(index_s + 1):].index(')')
                    text = text.replace(text[(index_ss + index_s + 1):(index_ee + index_s + 2)], '')
                index_e = text.index(')')
                text = text.replace(text[index_s:(index_e + 1)], '')
            if 'Unable to decode output address' in text:       #将无法解析地址去掉空格
                text=text.replace('Unable to decode output address','Unabletodecodeoutputaddress')

            while ',' in text:            #删除大额数字里面的“,”号
                text=text.replace(',','')
            list_text=text.split(' ')     #处理分离化为列表
            while '' in list_text:        #删除空元素
                list_text.remove('')
            Index=self.loc(list_text)
            Input=list_text[0:(Index-1)]
            Output=list_text[(Index-1):(len(list_text)-1)]
            Sum=[list_text[-1]]

            SUM.append(Sum)
            TIME.append(Time)
            INPUT.append(Input)
            OUTPUT.append(Output)
        infoDict['Sum'] = SUM
        infoDict['Time'] = TIME
        infoDict['Input'] = INPUT
        infoDict['Output'] = OUTPUT
        height=int(response.xpath('//div/div/div/table/tr/td/a[contains(@href,"block-height")]/text()').extract()[0])
        infoDict['Height']=[int(response.xpath('//div/div/div/table/tr/td/a[contains(@href,"block-height")]/text()').extract()[0])]*len(SUM)
        f = pd.DataFrame(infoDict)
        print(height)
        f.to_csv(r'e:\extract\%s-%s.csv' % (self.start,self.end-1), index=False,header=None, mode='a')#, mode='a'