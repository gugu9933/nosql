@echo off
chcp 65001 > nul
echo 正在启动带依赖单例模式服务器...

rem 确保配置文件存在
if not exist src\main\resources\jadis-single.properties (
    echo 创建单例模式配置文件...
    copy src\main\resources\jadis-master.properties src\main\resources\jadis-single.properties
    
    rem 修改为单例模式配置
    powershell -Command "(Get-Content src\main\resources\jadis-single.properties) -replace 'clusterEnabled=true', 'clusterEnabled=false' -replace 'port=6382', 'port=6379' | Set-Content src\main\resources\jadis-single.properties -Encoding UTF8"
)

rem 设置配置文件路径
set CONFIG_PATH=src/main/resources/jadis-single.properties

rem 使用Maven exec:java运行应用程序(包含所有依赖)
call mvn exec:java -Dexec.mainClass="com.redis.RedisServerStarter" -Dexec.args="-Djadis.config=%CONFIG_PATH%" -Dfile.encoding=UTF-8

pause 