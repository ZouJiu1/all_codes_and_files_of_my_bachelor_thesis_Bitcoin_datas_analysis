@echo off
:loop
tasklist | find /i "scrapy.exe"&&echo  [ %time:~,-3% ]存在进程scrapy.exe||(echo 开启scrapy进程中)&&(e:)&(cd E:\scrapy\Missing)&&(echo 进入到目录中)&&(scrapy crawl missing)
ping -n 30 127.0>nul
goto :loop