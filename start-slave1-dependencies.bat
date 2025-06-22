@echo off
chcp 65001 > nul
echo 正在启动带依赖从节点1服务器...
java -Djadis.config=src/main/resources/jadis-slave1.properties -jar target/nosql-1.0-SNAPSHOT-jar-with-dependencies.jar
pause