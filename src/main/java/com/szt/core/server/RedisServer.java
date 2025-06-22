package com.szt.core.server;

import com.szt.cluster.ClusterManager;
import com.szt.cluster.failover.FailoverHandler;
import com.szt.cluster.node.ClusterNode;
import com.szt.cluster.sync.DataSynchronizer;
import com.szt.command.CommandExecutor;
import com.szt.command.DefaultCommandExecutor;
import com.szt.common.exception.RedisException;
import com.szt.config.RedisConfig;
import com.szt.core.db.DatabaseManager;
import com.szt.core.network.RedisNetworkServer;
import com.szt.event.DefaultKeyEventPublisher;
import com.szt.event.KeyEventPublisher;
import lombok.Getter;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Redis服务器核心类
 * 负责服务器的启动、停止和管理
 */
public class RedisServer {

    /**
     * 服务器运行状态
     */
    @Getter
    private static volatile boolean running = false;

    /**
     * 服务器信息
     */
    @Getter
    private static ServerInfo serverInfo;

    /**
     * 服务器配置
     */
    @Getter
    private static RedisConfig config;

    /**
     * 服务器锁
     */
    private static final ReentrantLock LOCK = new ReentrantLock();

    /**
     * 事件发布器
     */
    @Getter
    private static KeyEventPublisher eventPublisher;

    /**
     * 数据库管理器
     */
    @Getter
    private static DatabaseManager databaseManager;

    /**
     * 集群管理器
     */
    @Getter
    private static ClusterManager clusterManager;

    /**
     * 故障转移处理器
     */
    @Getter
    private static FailoverHandler failoverHandler;

    /**
     * 数据同步器
     */
    @Getter
    private static DataSynchronizer dataSynchronizer;

    /**
     * 命令执行器
     */
    @Getter
    private static CommandExecutor commandExecutor;

    /**
     * 网络服务器
     */
    private static RedisNetworkServer networkServer;

    private RedisServer() {
        // 防止实例化
    }

    /**
     * 启动Redis服务器
     *
     * @param config 服务器配置
     */
    public static void start(RedisConfig config) {
        LOCK.lock();
        try {
            if (running) {
                throw new com.szt.common.exception.RedisException("Redis server is already running");
            }

            RedisServer.config = config;

            // 初始化事件发布器
            eventPublisher = new DefaultKeyEventPublisher();

            // 初始化服务器信息
            serverInfo = new ServerInfo();

            // 初始化数据库管理器
            databaseManager = new DatabaseManager(config, eventPublisher);
            databaseManager.initialize();

            // 注册事件监听器
            eventPublisher.addEventListener(serverInfo);

            // 初始化集群组件
            initializeClusterComponents();

            // 初始化命令执行器
            commandExecutor = new DefaultCommandExecutor(databaseManager, config.isClusterEnabled());

            // 启动网络服务
            startNetworkService();

            running = true;
        } catch (Exception e) {
            throw new com.szt.common.exception.RedisException("Failed to start Redis server", e);
        } finally {
            LOCK.unlock();
        }
    }

    /**
     * 初始化集群组件
     */
    private static void initializeClusterComponents() {
        // 只有在集群模式下才初始化集群组件
        if (!config.isClusterEnabled()) {
            return;
        }

        // 初始化集群管理器
        clusterManager = new ClusterManager(config);

        // 初始化故障转移处理器
        failoverHandler = new FailoverHandler(config, clusterManager);

        // 从配置中解析从节点列表
        if (config.isMaster() && config.getSlaveNodes() != null && !config.getSlaveNodes().isEmpty()) {
            String[] slaveNodeInfos = config.getSlaveNodes().split(",");
            for (String slaveInfo : slaveNodeInfos) {
                try {
                    String[] parts = slaveInfo.trim().split(":");
                    if (parts.length >= 3) {
                        String nodeId = parts[0].trim();
                        String host = parts[1].trim();
                        int port = Integer.parseInt(parts[2].trim());

                        // 创建从节点并添加到集群
                        ClusterNode slaveNode = new ClusterNode();
                        slaveNode.setNodeId(nodeId);
                        slaveNode.setHost(host);
                        slaveNode.setPort(port);
                        slaveNode.setRole(ClusterNode.NodeRole.SLAVE);
                        slaveNode.setStatus(ClusterNode.NodeStatus.ONLINE);
                        slaveNode.setMasterId(config.getNodeId());

                        clusterManager.addNode(slaveNode);
                    }
                } catch (Exception e) {
                    throw new RedisException("Invalid slave node configuration: " + slaveInfo, e);
                }
            }
        }

        // 如果是从节点，初始化数据同步器
        if (!config.isMaster()) {
            // 创建主节点信息
            ClusterNode masterNode = new ClusterNode();
            masterNode.setNodeId(config.getNodeRole().equals("slave") ? config.getMasterId() : "master");
            masterNode.setHost(config.getMasterHost());
            masterNode.setPort(config.getMasterPort());
            masterNode.setRole(ClusterNode.NodeRole.MASTER);
            masterNode.setStatus(ClusterNode.NodeStatus.ONLINE);

            // 将主节点添加到集群
            clusterManager.addNode(masterNode);

            // 初始化数据同步器
            dataSynchronizer = new DataSynchronizer(
                    config,
                    clusterManager.getClusterCommunicator(),
                    databaseManager.getDatabases(),
                    masterNode);
            dataSynchronizer.start();
        }
    }

    /**
     * 停止Redis服务器
     */
    public static void stop() {
        LOCK.lock();
        try {
            if (!running) {
                return;
            }

            // 停止集群组件
            if (dataSynchronizer != null) {
                dataSynchronizer.stop();
            }
            if (clusterManager != null) {
                clusterManager.shutdown();
            }

            // 停止网络服务
            stopNetworkService();

            // 持久化数据
            databaseManager.shutdown();

            running = false;
        } finally {
            LOCK.unlock();
        }
    }

    /**
     * 启动网络服务
     */
    private static void startNetworkService() {
        networkServer = new RedisNetworkServer(config, commandExecutor);
        networkServer.start();
    }

    /**
     * 停止网络服务
     */
    private static void stopNetworkService() {
        if (networkServer != null) {
            networkServer.stop();
        }
    }
}