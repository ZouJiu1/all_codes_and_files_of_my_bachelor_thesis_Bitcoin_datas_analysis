@echo off
set processname=scrapy.exe
set num=0
for /f "delims=" %%a in ('tasklist ^| find /i "%processname%"') do (
    if /i not "%%a" == "" (
        set /a num+=1
    )
)
echo 进程数量 %num%
if %num% leq 6 (
(echo 开启scrapy进程中)&&(e:)&(cd e:/fortest)&&(echo 进入到目录中)&&(scrapy crawl sample)
)
pause
