@echo off
chcp 65001 > nul
echo 正在启动带依赖的 Redis 集群模式...

rem 启动主节点
echo 启动主节点服务器...
start cmd /k "start-master-dependencies.bat"

rem 等待主节点启动
echo 等待主节点启动完成...
ping 127.0.0.1 -n 10 > nul

rem 启动从节点1
echo 启动从节点1...
start cmd /k "start-slave1-dependencies.bat"

rem 等待从节点1启动
ping 127.0.0.1 -n 5 > nul

rem 启动从节点2
echo 启动从节点2...
start cmd /k "start-slave2-dependencies.bat"

echo 集群已启动！
echo 主节点: 127.0.0.1:6380
echo 从节点1: 127.0.0.1:6381
echo 从节点2: 127.0.0.1:6382

pause 