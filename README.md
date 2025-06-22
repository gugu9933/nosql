# 命令行演示示例

本文档提供了使用本 Redis 实现的各种数据类型命令的示例。

## 目录

- [字符串操作](#字符串操作)
- [列表操作](#列表操作)
- [哈希操作](#哈希操作)
- [集合操作](#集合操作)
- [有序集合操作](#有序集合操作)
- [键管理操作](#键管理操作)
- [集群操作](#集群操作)
- [持久化与恢复](#持久化与恢复)

## 部署架构

该系统支持单机模式和集群模式：

1. **单机模式**: 单节点运行，适合开发和测试环境
2. **集群模式**: 主从复制架构，一个主节点和多个从节点
   - 主节点支持读写操作
   - 从节点只支持读操作
   - 从节点通过同步机制和持久化文件从主节点复制数据

## 启动服务

### 主节点启动

```bash
# 使用Maven打包
mvn clean package

# 启动主节点
start-master-dependencies.bat
```

### 从节点启动

```bash
# 启动从节点1
start-slave1-dependencies.bat

# 启动从节点2
start-slave2-dependencies.bat
```

## 字符串操作

字符串是最基本的数据类型，可以存储文本、整数或二进制数据。

### SET - 设置字符串

```
127.0.0.1:6380 (主节点)> SET name "张三"
OK
```

### GET - 获取字符串

```
127.0.0.1:6380 (主节点)> GET name
"张三"
```

### GETSET - 设置新值并返回旧值

```
127.0.0.1:6380 (主节点)> GETSET name "李四"
"张三"

127.0.0.1:6380 (主节点)> GET name
"李四"
```

### INCR - 递增数值

```
127.0.0.1:6380 (主节点)> SET counter 10
OK

127.0.0.1:6380 (主节点)> INCR counter
(integer) 11
```

### INCRBY - 按指定量递增数值

```
127.0.0.1:6380 (主节点)> INCRBY counter 5
(integer) 16
```

### DECR - 递减数值

```
127.0.0.1:6380 (主节点)> DECR counter
(integer) 15
```

### DECRBY - 按指定量递减数值

```
127.0.0.1:6380 (主节点)> DECRBY counter 5
(integer) 10
```

## 列表操作

列表是简单的字符串列表，按照插入顺序排序。

### LPUSH - 在列表头部添加元素

```
127.0.0.1:6380 (主节点)> LPUSH fruits "apple"
(integer) 1

127.0.0.1:6380 (主节点)> LPUSH fruits "banana" "orange"
(integer) 3
```

### RPUSH - 在列表尾部添加元素

```
127.0.0.1:6380 (主节点)> RPUSH fruits "pear"
(integer) 4
```

### LRANGE - 获取列表范围内的元素

```
127.0.0.1:6380 (主节点)> LRANGE fruits 0 -1
1) "orange"
2) "banana"
3) "apple"
4) "pear"
```

### LPOP - 移除并返回列表头部的元素

```
127.0.0.1:6380 (主节点)> LPOP fruits
"orange"
```

### RPOP - 移除并返回列表尾部的元素

```
127.0.0.1:6380 (主节点)> RPOP fruits
"pear"
```

### LLEN - 获取列表长度

```
127.0.0.1:6380 (主节点)> LLEN fruits
(integer) 2
```

### LINDEX - 通过索引获取列表元素

```
127.0.0.1:6380 (主节点)> LINDEX fruits 0
"banana"
```

### LSET - 通过索引设置列表元素

```
127.0.0.1:6380 (主节点)> LSET fruits 0 "grape"
OK

127.0.0.1:6380 (主节点)> LRANGE fruits 0 -1
1) "grape"
2) "apple"
```

## 哈希操作

哈希是键值对的集合，适合存储对象。

### HSET - 设置哈希字段的值

```
127.0.0.1:6380 (主节点)> HSET user name "李四"
(integer) 1

127.0.0.1:6380 (主节点)> HSET user age 25 city "北京"
(integer) 2
```

### HGET - 获取哈希字段的值

```
127.0.0.1:6380 (主节点)> HGET user name
"李四"
```

### HGETALL - 获取哈希表中所有的字段和值

```
127.0.0.1:6380 (主节点)> HGETALL user
1) "name"
2) "李四"
3) "age"
4) "25"
5) "city"
6) "北京"
```

### HMSET - 设置多个哈希字段

```
127.0.0.1:6380 (主节点)> HMSET product name "手机" price 3999 brand "华为"
OK
```


### HDEL - 删除一个或多个哈希字段

```
127.0.0.1:6380 (主节点)> HDEL user city
(integer) 1
```

### HEXISTS - 检查字段是否存在

```
127.0.0.1:6380 (主节点)> HEXISTS user name
(integer) 1

127.0.0.1:6380 (主节点)> HEXISTS user city
(integer) 0
```

### HLEN - 获取哈希表中字段的数量

```
127.0.0.1:6380 (主节点)> HLEN user
(integer) 2
```

### HKEYS - 获取哈希表中所有的字段

```
127.0.0.1:6380 (主节点)> HKEYS user
1) "name"
2) "age"
```

### HVALS - 获取哈希表中所有的值

```
127.0.0.1:6380 (主节点)> HVALS user
1) "李四"
2) "25"
```

## 集合操作

集合是无序的字符串集合，不允许重复成员。

### SADD - 添加一个或多个集合成员

```
127.0.0.1:6380 (主节点)> SADD colors "red" "green" "blue"
(integer) 3
```

### SMEMBERS - 获取集合中的所有成员

```
127.0.0.1:6380 (主节点)> SMEMBERS colors
1) "blue"
2) "green"
3) "red"
```

### SISMEMBER - 判断成员是否存在于集合中

```
127.0.0.1:6380 (主节点)> SISMEMBER colors "red"
(integer) 1

127.0.0.1:6380 (主节点)> SISMEMBER colors "yellow"
(integer) 0
```

### SCARD - 获取集合中成员的数量

```
127.0.0.1:6380 (主节点)> SCARD colors
(integer) 3
```

### SREM - 移除一个或多个集合成员

```
127.0.0.1:6380 (主节点)> SREM colors "red"
(integer) 1
```

### SPOP - 随机移除并返回集合中的一个或多个成员

```
127.0.0.1:6380 (主节点)> SPOP colors
"green"
```

### SINTER - 返回多个集合的交集

```
127.0.0.1:6380 (主节点)> SADD set1 "a" "b" "c"
(integer) 3

127.0.0.1:6380 (主节点)> SADD set2 "b" "c" "d"
(integer) 3

127.0.0.1:6380 (主节点)> SINTER set1 set2
1) "b"
2) "c"
```

### SUNION - 返回多个集合的并集

```
127.0.0.1:6380 (主节点)> SUNION set1 set2
1) "a"
2) "b"
3) "c"
4) "d"
```

### SDIFF - 返回多个集合的差集

```
127.0.0.1:6380 (主节点)> SDIFF set1 set2
1) "a"
```

## 有序集合操作

有序集合类似于集合，但每个成员关联一个分数，用于排序。

### ZADD - 添加一个或多个成员到有序集合，或更新已存在成员的分数

```
127.0.0.1:6380 (主节点)> ZADD scores 89 "Alice" 95 "Bob" 78 "Carol"
(integer) 3
```

### ZRANGE - 通过索引区间返回有序集合中的成员

```
127.0.0.1:6380 (主节点)> ZRANGE scores 0 -1
1) "Carol"
2) "Alice"
3) "Bob"
```

### ZRANGE WITHSCORES - 返回有序集合中的成员和分数

```
127.0.0.1:6380 (主节点)> ZRANGE scores 0 -1 WITHSCORES
1) "Carol"
2) "78"
3) "Alice"
4) "89"
5) "Bob"
6) "95"
```

### ZRANGEBYSCORE - 通过分数返回有序集合的成员

```
127.0.0.1:6380 (主节点)> ZRANGEBYSCORE scores 80 100
1) "Alice"
2) "Bob"
```

### ZREM - 移除有序集合中的一个或多个成员

```
127.0.0.1:6380 (主节点)> ZREM scores "Alice"
(integer) 1
```

### ZSCORE - 返回有序集合中成员的分数

```
127.0.0.1:6380 (主节点)> ZSCORE scores "Bob"
"95"
```

### ZRANK - 返回有序集合中成员的排名

```
127.0.0.1:6380 (主节点)> ZRANK scores "Bob"
(integer) 1
```

### ZCOUNT - 计算在有序集合中指定分数区间的成员数量

```
127.0.0.1:6380 (主节点)> ZCOUNT scores 70 90
(integer) 1
```

## 键管理操作

### EXISTS - 检查键是否存在

```
127.0.0.1:6380 (主节点)> EXISTS name
(integer) 1
```

### DEL - 删除键

```
127.0.0.1:6380 (主节点)> DEL name
(integer) 1
```

### TYPE - 返回键所存储值的类型

```
127.0.0.1:6380 (主节点)> TYPE user
hash
```

### EXPIRE - 设置键过期时间

```
127.0.0.1:6380 (主节点)> EXPIRE user 60
(integer) 1
```

### TTL - 获取键的剩余过期时间

```
127.0.0.1:6380 (主节点)> TTL user
(integer) 57
```

### PERSIST - 移除键的过期时间

```
127.0.0.1:6380 (主节点)> PERSIST user
(integer) 1
```

### KEYS - 查找所有匹配的键

```
127.0.0.1:6380 (主节点)> KEYS *
1) "user"
2) "scores"
3) "colors"
4) "fruits"
```

### FLUSHDB - 删除当前数据库中的所有键

```
127.0.0.1:6380 (主节点)> FLUSHDB
OK
```

### SELECT - 切换到指定的数据库

```
127.0.0.1:6380 (主节点)> SELECT 1
OK

127.0.0.1:6380[1] (主节点)>
```

## 集群操作

集群模式下的读写操作示例：

### 主节点操作 (支持读写)

```
127.0.0.1:6380 (主节点)> SET msg "Hello from master"
OK

127.0.0.1:6380 (主节点)> GET msg
"Hello from master"
```

### 从节点操作 (只读)

```
127.0.0.1:6381 (从节点1)> GET msg
"Hello from master"

127.0.0.1:6381 (从节点1)> SET msg "try to write"
(error) READONLY You can't write against a read only slave.
```

### 集群信息查看

```
127.0.0.1:6380 (主节点)> INFO REPLICATION
# Replication
role:master
connected_slaves:2
slave0:127.0.0.1,6381,online
slave1:127.0.0.1,6382,online
```

## 持久化与恢复

### AOF 持久化示例

AOF (Append Only File) 模式下，每个命令都会被追加到 appendonly.aof 文件中：

```
# 查看持久化模式
127.0.0.1:6380 (主节点)> CONFIG GET persistenceMode
1) "persistenceMode"
2) "aof"

# 设置一些数据
127.0.0.1:6380 (主节点)> SET key1 "value1"
OK

127.0.0.1:6380 (主节点)> SET key2 "value2"
OK

# 此时 appendonly.aof 文件已包含这些命令
```

服务器重启后，所有数据会从 AOF 文件恢复：

```
# 重启服务器后
127.0.0.1:6380 (主节点)> GET key1
"value1"
```

### RDB 持久化示例

RDB (Redis Database) 模式下，会定期生成数据快照到 dump.rdb 文件：

```
# 查看持久化模式
127.0.0.1:6380 (主节点)> CONFIG GET persistenceMode
1) "persistenceMode"
2) "rdb"

# 手动触发保存
127.0.0.1:6380 (主节点)> SAVE
OK

# 此时 dump.rdb 文件已更新
```

服务器重启后，所有数据会从 RDB 文件恢复：

```
# 重启服务器后
127.0.0.1:6380 (主节点)> GET key1
"value1"
```

## 完整示例会话

下面是一个完整的命令行交互示例：

```
连接到Redis主节点: 127.0.0.1:6380
主节点支持所有读写操作
+OK Welcome to Java-Redis Server
请输入Redis命令（输入'exit'退出）：

127.0.0.1:6380 (主节点)> SET user:1:name "张三"
OK

127.0.0.1:6380 (主节点)> GET user:1:name
"张三"

127.0.0.1:6380 (主节点)> HSET user:1 name "张三" age 30 city "上海"
(integer) 3

127.0.0.1:6380 (主节点)> HGETALL user:1
1) "name"
2) "张三"
3) "age"
4) "30"
5) "city"
6) "上海"

127.0.0.1:6380 (主节点)> LPUSH notifications "新消息1" "新消息2"
(integer) 2

127.0.0.1:6380 (主节点)> LRANGE notifications 0 -1
1) "新消息2"
2) "新消息1"

127.0.0.1:6380 (主节点)> SADD tags "java" "redis" "nosql"
(integer) 3

127.0.0.1:6380 (主节点)> SMEMBERS tags
1) "nosql"
2) "redis"
3) "java"

127.0.0.1:6380 (主节点)> ZADD scores 95 "小明" 88 "小红" 92 "小李"
(integer) 3

127.0.0.1:6380 (主节点)> ZRANGE scores 0 -1 WITHSCORES
1) "小红"
2) "88"
3) "小李"
4) "92"
5) "小明"
6) "95"

127.0.0.1:6380 (主节点)> EXPIRE user:1 60
(integer) 1

127.0.0.1:6380 (主节点)> TTL user:1
(integer) 58
```

---

## 从节点数据同步示例

数据会从主节点自动同步到从节点：

```
# 在主节点设置数据
127.0.0.1:6380 (主节点)> SET sync_test "hello from master"
OK

# 在从节点查看数据（几秒后数据会同步）
127.0.0.1:6381 (从节点1)> GET sync_test
"hello from master"
```
