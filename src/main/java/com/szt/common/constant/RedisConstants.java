package com.szt.common.constant;

/**
 * Redis系统常量定义
 */
public final class RedisConstants {

    private RedisConstants() {
        // 防止实例化
    }

    // 数据过期相关常量
    public static final int NEVER_EXPIRES = -1;

    // 数据类型常量
    public static final String TYPE_STRING = "string";
    public static final String TYPE_LIST = "list";
    public static final String TYPE_SET = "set";
    public static final String TYPE_ZSET = "zset";
    public static final String TYPE_HASH = "hash";

    // 持久化相关常量
    public static final String PERSISTENCE_RDB = "rdb";
    public static final String PERSISTENCE_AOF = "aof";

    // 集群相关常量
    public static final String ROLE_MASTER = "master";
    public static final String ROLE_SLAVE = "slave";
    public static final String ROLE_UNKNOWN = "unknown";

    // 配置相关常量
    public static final String CONFIG_FILE_NAME = "jadis.properties";
    public static final String CONFIG_DIR = "config";
    public static final String CONFIG_SYSTEM_PROPERTY = "jadis.config";

    // 默认配置值
    public static final int DEFAULT_PORT = 6379;
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_DATABASE_COUNT = 16;

    // AOF相关默认值
    public static final String DEFAULT_AOF_FSYNC = "everysec";
    public static final long DEFAULT_AOF_REWRITE_SIZE = 64 * 1024 * 1024; // 64MB

    // RDB相关默认值
    public static final int DEFAULT_RDB_SAVE_INTERVAL = 60; // 60秒
    public static final boolean DEFAULT_RDB_COMPRESSION = true;
}