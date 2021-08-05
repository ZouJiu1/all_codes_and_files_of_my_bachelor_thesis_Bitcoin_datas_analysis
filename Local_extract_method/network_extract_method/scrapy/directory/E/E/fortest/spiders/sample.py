    # -*- coding: utf-8 -*-
import scrapy
from scrapy.http import FormRequest

class SampleSpider(scrapy.Spider):
    name = 'sample'
    def start_requests(self):
        x = 'https://blockchain.info/search?search='
        urls=[]
        for i in range(400000,416000,1):
            urls.append(x+str(i))
        # urls = ['https://blockchain.info/block/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f']
        for url in urls:
            yield scrapy.Request(url=url,callback=self.parse)

    def parse(self, response):
        next_h = response.xpath('//div/div/div/table/tr/td/a[contains(@class,"hash-link")]/text()').extract()
        if len(next_h) == 4:
            yield FormRequest(url='https://blockchain.info/block/' + next_h[3], meta={'next_h3': next_h[3]},formdata={})
        try:
            next_href = 'https://blockchain.info/block/' + next_h[2]
        except:
            print('有分支出现，需要进行筛选！')
            next_href = 'https://blockchain.info/block/' + response.meta["next_h3"]
            print(next_href)
            yield scrapy.Request(next_href, callback=self.parse)
        height=int(response.xpath('//div/div/div/table/tr/td/a[contains(@href,"block-height")]/text()').extract()[0])
        # print('Height:',height)
        with open('e:\esave\%s.html' % height,'wb') as f:
            f.write(response.body)