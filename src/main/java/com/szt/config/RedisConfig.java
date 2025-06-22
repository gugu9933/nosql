package com.szt.config;

import com.szt.common.constant.RedisConstants;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis配置类
 */
@Data
public class RedisConfig {
    private static final Logger logger = LoggerFactory.getLogger(RedisConfig.class);

    /**
     * 服务器配置
     */
    private String host;
    private int port;
    private int databaseCount;
    private String persistenceMode;

    /**
     * 集群配置
     */
    private boolean clusterEnabled;
    private String nodeId;
    private String nodeRole;
    private String masterHost;
    private int masterPort;
    private String masterId;
    private int heartbeatInterval;
    private int nodeStatusInterval;
    private int nodeTimeout;
    private int syncInterval;
    private String slaveNodes;
    private int syncConnectTimeout;
    private int syncReadTimeout;

    /**
     * RDB配置
     */
    private boolean rdbCompression;
    private int rdbSaveInterval;

    /**
     * AOF配置
     */
    private String aofFsync;
    private long aofRewriteSize;

    /**
     * 获取当前节点是否为主节点
     */
    public boolean isMaster() {
        return RedisConstants.ROLE_MASTER.equalsIgnoreCase(nodeRole);
    }

    /**
     * 获取当前节点是否为从节点
     */
    public boolean isSlave() {
        return RedisConstants.ROLE_SLAVE.equalsIgnoreCase(nodeRole);
    }

    /**
     * 输出配置信息，用于调试
     */
    public void logConfig() {
        logger.info("Redis配置信息:");
        logger.info("节点ID: {}", nodeId);
        logger.info("节点角色: {}", nodeRole);
        logger.info("主机地址: {}:{}", host, port);
        logger.info("集群启用: {}", clusterEnabled);

        if (isSlave()) {
            logger.info("主节点ID: {}", masterId);
            logger.info("主节点地址: {}:{}", masterHost, masterPort);
        } else if (isMaster()) {
            logger.info("从节点列表: {}", slaveNodes);
        }

        logger.info("心跳间隔: {}秒", heartbeatInterval);
        logger.info("节点状态检查间隔: {}秒", nodeStatusInterval);
        logger.info("节点超时时间: {}秒", nodeTimeout);
    }
}