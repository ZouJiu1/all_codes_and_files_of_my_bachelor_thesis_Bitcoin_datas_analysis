@echo off
:loop
set name=scrapy.exe
set num=0
for /f "delims=" %%a in ('tasklist ^| find /i "%name%"') do (
    if /i not "%%a" == "" (
        set /a num+=1
    )
)
echo 进程数量 %num%
if %num% leq 6 (
(echo 开启scrapy进程中)&&(F:)&(cd F:/fortest400000)&&(echo 进入到目录中)&&(scrapy crawl sample400000)
)
ping -n 10 127.0>nul
goto :loop
