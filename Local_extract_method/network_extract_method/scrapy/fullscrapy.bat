@echo off
:loop
tasklist | find /i "scrapy.exe"&&echo  [ %time:~,-3% ]存在进程scrapy.exe||(echo 开启scrapy进程中)&&(e:)&(cd e:/scrapy/fortest)&&(echo 进入到目录中)&&(scrapy crawl sample)
ping -n 10 127.0>nul
goto :loop