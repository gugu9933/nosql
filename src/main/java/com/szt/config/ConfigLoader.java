package com.szt.config;

import com.szt.common.constant.RedisConstants;
import com.szt.common.exception.RedisConfigException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 配置加载器
 */
public class ConfigLoader {

    private ConfigLoader() {
        // 防止实例化
    }

    /**
     * 加载配置
     */
    public static RedisConfig loadConfig() {
        Properties properties = loadProperties();
        return buildConfig(properties);
    }

    /**
     * 加载配置文件
     */
    private static Properties loadProperties() {
        Properties properties = new Properties();

        // 1. 检查系统属性指定的配置文件
        String configPath = System.getProperty(RedisConstants.CONFIG_SYSTEM_PROPERTY);
        if (configPath != null && !configPath.isEmpty()) {
            try {
                properties.load(Files.newInputStream(Paths.get(configPath)));
                return properties;
            } catch (IOException e) {
                throw new RedisConfigException("Failed to load config file from system property: " + configPath, e);
            }
        }

        // 2. 按照默认顺序查找配置文件
        Path jarPath = Paths.get(ConfigLoader.class.getProtectionDomain()
                .getCodeSource().getLocation().getPath().substring(1));

        // 优先读取jar同级目录配置文件
        Path configPath1 = jarPath.getParent().resolve(RedisConstants.CONFIG_FILE_NAME);
        try {
            properties.load(Files.newInputStream(configPath1));
            return properties;
        } catch (IOException e) {
            // 尝试读取jar同级config目录配置文件
            Path configPath2 = jarPath.getParent()
                    .resolve(RedisConstants.CONFIG_DIR + File.separator + RedisConstants.CONFIG_FILE_NAME);
            try {
                properties.load(Files.newInputStream(configPath2));
                return properties;
            } catch (IOException ex) {
                // 最后尝试读取classpath配置文件
                try {
                    properties.load(ConfigLoader.class.getClassLoader()
                            .getResourceAsStream(RedisConstants.CONFIG_FILE_NAME));
                    return properties;
                } catch (IOException | NullPointerException exc) {
                    throw new RedisConfigException("No config file found", exc);
                }
            }
        }
    }

    /**
     * 根据Properties构建配置对象
     */
    private static RedisConfig buildConfig(Properties properties) {
        RedisConfig config = new RedisConfig();

        // 服务器配置
        config.setHost(properties.getProperty("host", RedisConstants.DEFAULT_HOST));
        config.setPort(Integer.parseInt(properties.getProperty("port",
                String.valueOf(RedisConstants.DEFAULT_PORT))));
        config.setDatabaseCount(Integer.parseInt(properties.getProperty("databaseCount",
                String.valueOf(RedisConstants.DEFAULT_DATABASE_COUNT))));

        // 持久化配置
        config.setPersistenceMode(properties.getProperty("persistenceMode",
                RedisConstants.PERSISTENCE_RDB));

        // RDB配置
        config.setRdbSaveInterval(Integer.parseInt(properties.getProperty("rdbSaveInterval",
                String.valueOf(RedisConstants.DEFAULT_RDB_SAVE_INTERVAL))));
        config.setRdbCompression(Boolean.parseBoolean(properties.getProperty("rdbCompression",
                String.valueOf(RedisConstants.DEFAULT_RDB_COMPRESSION))));

        // AOF配置
        config.setAofFsync(properties.getProperty("aofFsync",
                RedisConstants.DEFAULT_AOF_FSYNC));
        config.setAofRewriteSize(Long.parseLong(properties.getProperty("aofRewriteSize",
                String.valueOf(RedisConstants.DEFAULT_AOF_REWRITE_SIZE))));

        // 集群配置
        config.setClusterEnabled(Boolean.parseBoolean(properties.getProperty("clusterEnabled", "false")));
        config.setNodeRole(properties.getProperty("nodeRole", RedisConstants.ROLE_MASTER));
        config.setNodeId(properties.getProperty("nodeId", "node1"));
        config.setMasterHost(properties.getProperty("masterHost"));
        config.setMasterPort(Integer.parseInt(properties.getProperty("masterPort", "6379")));
        config.setMasterId(properties.getProperty("masterId", "master"));
        config.setHeartbeatInterval(Integer.parseInt(properties.getProperty("heartbeatInterval", "5")));
        config.setNodeStatusInterval(Integer.parseInt(properties.getProperty("nodeStatusInterval", "10")));
        config.setNodeTimeout(Integer.parseInt(properties.getProperty("nodeTimeout", "30")));
        config.setSlaveNodes(properties.getProperty("slaveNodes"));
        config.setSyncInterval(Integer.parseInt(properties.getProperty("syncInterval", "5")));
        config.setSyncConnectTimeout(Integer.parseInt(properties.getProperty("syncConnectTimeout", "5000")));
        config.setSyncReadTimeout(Integer.parseInt(properties.getProperty("syncReadTimeout", "60000")));

        return config;
    }
}