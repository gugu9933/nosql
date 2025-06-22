package com.szt.cluster;

import com.szt.cluster.communication.ClusterCommunicator;
import com.szt.cluster.communication.ClusterMessage;
import com.szt.cluster.node.ClusterNode;
import com.szt.config.RedisConfig;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 集群管理器
 * 负责管理集群节点和集群状态
 */
public class ClusterManager {
    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    /**
     * 配置信息
     */
    private final RedisConfig config;

    /**
     * 当前节点
     */
    @Getter
    private final ClusterNode currentNode;

    /**
     * 集群节点映射表
     */
    @Getter
    private final Map<String, ClusterNode> nodes;

    /**
     * 定时任务执行器
     */
    private final ScheduledExecutorService scheduler;

    /**
     * 集群通信器
     */
    @Getter
    private final ClusterCommunicator clusterCommunicator;

    /**
     * 是否启用集群功能
     */
    private final boolean clusterEnabled;

    /**
     * 构造函数
     *
     * @param config 配置信息
     */
    public ClusterManager(RedisConfig config) {
        this.config = config;
        this.nodes = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.clusterEnabled = config.isClusterEnabled();

        // 初始化当前节点
        this.currentNode = initializeCurrentNode();

        // 如果是主节点，初始化从节点配置
        if (clusterEnabled && config.isMaster()) {
            initializeSlaveNodes();
        }

        // 初始化集群通信器
        this.clusterCommunicator = new ClusterCommunicator(config, this::handleClusterMessage);

        // 启动集群管理任务
        if (clusterEnabled) {
            startClusterTasks();
        }
    }

    /**
     * 初始化当前节点
     */
    private ClusterNode initializeCurrentNode() {
        ClusterNode node = new ClusterNode();
        node.setNodeId(config.getNodeId());
        node.setHost(config.getHost());
        node.setPort(config.getPort());
        node.setRole(config.isMaster() ? ClusterNode.NodeRole.MASTER : ClusterNode.NodeRole.SLAVE);
        node.setStatus(ClusterNode.NodeStatus.ONLINE);
        node.updateHeartbeat();

        if (!config.isMaster()) {
            // 如果是从节点,设置主节点ID
            node.setMasterId(config.getMasterId());
        }

        // 将当前节点添加到节点映射表
        nodes.put(node.getNodeId(), node);

        return node;
    }

    /**
     * 初始化从节点配置
     */
    private void initializeSlaveNodes() {
        if (config.getSlaveNodes() == null || config.getSlaveNodes().isEmpty()) {
            return;
        }

        try {
            // 解析从节点配置，格式：nodeId:host:port,nodeId:host:port...
            String[] slaveConfigs = config.getSlaveNodes().split(",");
            for (String slaveConfig : slaveConfigs) {
                String[] parts = slaveConfig.trim().split(":");
                if (parts.length >= 3) {
                    String nodeId = parts[0].trim();
                    String host = parts[1].trim();
                    int port = Integer.parseInt(parts[2].trim());

                    ClusterNode slaveNode = new ClusterNode();
                    slaveNode.setNodeId(nodeId);
                    slaveNode.setHost(host);
                    slaveNode.setPort(port);
                    slaveNode.setRole(ClusterNode.NodeRole.SLAVE);
                    slaveNode.setMasterId(currentNode.getNodeId());
                    slaveNode.setStatus(ClusterNode.NodeStatus.OFFLINE); // 初始状态为离线

                    nodes.put(nodeId, slaveNode);
                }
            }
        } catch (Exception e) {
            // 静默处理异常
        }
    }

    /**
     * 启动集群管理任务
     */
    private void startClusterTasks() {
        // 启动通信服务
        clusterCommunicator.start();

        // 心跳检测任务
        scheduler.scheduleWithFixedDelay(
                this::heartbeatCheck,
                0,
                config.getHeartbeatInterval(),
                TimeUnit.SECONDS);

        // 节点状态更新任务
        scheduler.scheduleWithFixedDelay(
                this::updateNodeStatus,
                0,
                config.getNodeStatusInterval(),
                TimeUnit.SECONDS);
    }

    /**
     * 处理集群消息
     */
    private void handleClusterMessage(ClusterMessage message) {
        try {
            switch (message.getType()) {
                case HEARTBEAT:
                    handleHeartbeat(message);
                    break;
                case PING:
                    handlePing(message);
                    break;
                case PONG:
                    handlePong(message);
                    break;
                case NODE_ADDED:
                case NODE_REMOVED:
                    handleNodeChange(message);
                    break;
                default:
                    // 静默处理未知消息类型
            }
        } catch (Exception e) {
            // 静默处理异常
        }
    }

    /**
     * 处理心跳消息
     */
    private void handleHeartbeat(ClusterMessage message) {
        String nodeId = message.getSenderId();
        ClusterNode node = nodes.get(nodeId);
        if (node != null) {
            node.updateHeartbeat();
            node.setStatus(ClusterNode.NodeStatus.ONLINE);
        }
    }

    /**
     * 处理Ping消息
     */
    private void handlePing(ClusterMessage message) {
        // 回复Pong
        ClusterMessage pong = ClusterMessage.create(
                ClusterMessage.MessageType.PONG,
                currentNode.getNodeId(),
                message.getSenderId(),
                null);
        ClusterNode sender = nodes.get(message.getSenderId());
        if (sender != null) {
            clusterCommunicator.sendMessage(pong, sender);
        }
    }

    /**
     * 处理Pong消息
     */
    private void handlePong(ClusterMessage message) {
        String nodeId = message.getSenderId();
        ClusterNode node = nodes.get(nodeId);
        if (node != null) {
            node.updateHeartbeat();
            node.setStatus(ClusterNode.NodeStatus.ONLINE);
        }
    }

    /**
     * 处理节点变更消息
     */
    private void handleNodeChange(ClusterMessage message) {
        if (message.getType() == ClusterMessage.MessageType.NODE_ADDED) {
            ClusterNode newNode = (ClusterNode) message.getPayload();
            if (newNode != null && !newNode.getNodeId().equals(currentNode.getNodeId())) {
                addNode(newNode);
            }
        } else if (message.getType() == ClusterMessage.MessageType.NODE_REMOVED) {
            String nodeId = (String) message.getPayload();
            if (nodeId != null && !nodeId.equals(currentNode.getNodeId())) {
                removeNode(nodeId);
            }
        }
    }

    /**
     * 心跳检测
     */
    private void heartbeatCheck() {
        try {
            // 发送心跳到所有节点
            ClusterMessage heartbeat = ClusterMessage.create(
                    ClusterMessage.MessageType.HEARTBEAT,
                    currentNode.getNodeId(),
                    null,
                    null);

            for (ClusterNode node : nodes.values()) {
                if (!node.getNodeId().equals(currentNode.getNodeId())) {
                    try {
                        clusterCommunicator.sendMessage(heartbeat, node);
                    } catch (Exception e) {
                        // 静默处理异常
                    }
                }
            }

            // 检查节点状态
            long timeoutSeconds = config.getNodeTimeout();
            nodes.values().forEach(node -> {
                if (!node.getNodeId().equals(currentNode.getNodeId())) {
                    if (!node.isAlive(timeoutSeconds)) {
                        if (node.getStatus() != ClusterNode.NodeStatus.SUSPECT &&
                                node.getStatus() != ClusterNode.NodeStatus.OFFLINE) {
                            node.setStatus(ClusterNode.NodeStatus.SUSPECT);
                        }
                    }
                }
            });
        } catch (Exception e) {
            // 静默处理异常
        }
    }

    /**
     * 更新节点状态
     */
    private void updateNodeStatus() {
        try {
            nodes.values().forEach(node -> {
                if (!node.getNodeId().equals(currentNode.getNodeId())) {
                    if (node.getStatus() == ClusterNode.NodeStatus.SUSPECT) {
                        try {
                            // 发送PING检查节点是否真的下线
                            ClusterMessage ping = ClusterMessage.create(
                                    ClusterMessage.MessageType.PING,
                                    currentNode.getNodeId(),
                                    node.getNodeId(),
                                    null);
                            clusterCommunicator.sendMessage(ping, node);

                            // 如果超过一定时间仍然没有响应，标记为离线
                            if (!node.isAlive(config.getNodeTimeout() * 2)) {
                                if (node.getStatus() != ClusterNode.NodeStatus.OFFLINE) {
                                    node.setStatus(ClusterNode.NodeStatus.OFFLINE);
                                }
                            }
                        } catch (Exception e) {
                            // 静默处理异常
                        }
                    }
                }
            });
        } catch (Exception e) {
            // 静默处理异常
        }
    }

    /**
     * 添加节点
     *
     * @param node 节点信息
     */
    public void addNode(ClusterNode node) {
        // 检查节点是否已存在，避免重复添加
        if (nodes.containsKey(node.getNodeId())) {
            ClusterNode existingNode = nodes.get(node.getNodeId());
            // 更新现有节点的状态
            existingNode.setStatus(node.getStatus());
            existingNode.updateHeartbeat();
            return;
        }

        nodes.put(node.getNodeId(), node);

        // 广播节点加入消息
        broadcastNodeChange(ClusterMessage.MessageType.NODE_ADDED, node);
    }

    /**
     * 移除节点
     *
     * @param nodeId 节点ID
     */
    public void removeNode(String nodeId) {
        if (!nodes.containsKey(nodeId)) {
            return;
        }

        nodes.remove(nodeId);

        // 广播节点移除消息
        broadcastNodeChange(ClusterMessage.MessageType.NODE_REMOVED, nodeId);
    }

    /**
     * 广播节点变更消息
     */
    private void broadcastNodeChange(ClusterMessage.MessageType type, Object payload) {
        ClusterMessage message = ClusterMessage.create(type, currentNode.getNodeId(), null, payload);
        for (ClusterNode node : nodes.values()) {
            if (!node.getNodeId().equals(currentNode.getNodeId())) {
                clusterCommunicator.sendMessage(message, node);
            }
        }
    }

    /**
     * 关闭集群管理器
     */
    public void shutdown() {
        clusterCommunicator.stop();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}