package com.szt.cluster.failover;

import com.szt.cluster.ClusterManager;
import com.szt.cluster.communication.ClusterMessage;
import com.szt.cluster.node.ClusterNode;
import com.szt.config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 故障转移处理器
 * 负责处理主节点故障时的故障转移
 */
public class FailoverHandler {
    private static final Logger logger = LoggerFactory.getLogger(FailoverHandler.class);

    /**
     * 配置信息
     */
    private final RedisConfig config;

    /**
     * 集群管理器
     */
    private final ClusterManager clusterManager;

    /**
     * 构造函数
     *
     * @param config         配置信息
     * @param clusterManager 集群管理器
     */
    public FailoverHandler(RedisConfig config, ClusterManager clusterManager) {
        this.config = config;
        this.clusterManager = clusterManager;
    }

    /**
     * 处理主节点故障
     *
     * @param failedMaster 故障的主节点
     */
    public void handleMasterFailure(ClusterNode failedMaster) {
        try {
            logger.info("Handling master failure: {}", failedMaster.getNodeId());

            // 获取所有从节点
            List<ClusterNode> slaves = clusterManager.getNodes().values().stream()
                    .filter(node -> node.getRole() == ClusterNode.NodeRole.SLAVE)
                    .filter(node -> failedMaster.getNodeId().equals(node.getMasterId()))
                    .filter(node -> node.getStatus() == ClusterNode.NodeStatus.ONLINE)
                    .collect(Collectors.toList());

            if (slaves.isEmpty()) {
                logger.error("No available slave nodes for failover");
                return;
            }

            // 选择新的主节点(这里简单地选择最后一次心跳时间最新的从节点)
            ClusterNode newMaster = slaves.stream()
                    .max(Comparator.comparing(ClusterNode::getLastHeartbeat))
                    .orElse(null);

            if (newMaster == null) {
                logger.error("Failed to select new master node");
                return;
            }

            // 开始故障转移
            startFailover(failedMaster, newMaster);

        } catch (Exception e) {
            logger.error("Error handling master failure", e);
        }
    }

    /**
     * 开始故障转移
     *
     * @param failedMaster 故障的主节点
     * @param newMaster    新的主节点
     */
    private void startFailover(ClusterNode failedMaster, ClusterNode newMaster) {
        try {
            // 广播故障转移开始消息
            broadcastFailoverStart(failedMaster, newMaster);

            // 将选中的从节点提升为主节点
            promoteToMaster(newMaster);

            // 更新其他从节点的主节点信息
            updateSlaves(failedMaster, newMaster);

            // 广播故障转移完成消息
            broadcastFailoverEnd(newMaster);

            logger.info("Failover completed. New master: {}", newMaster.getNodeId());
        } catch (Exception e) {
            logger.error("Error during failover", e);
        }
    }

    /**
     * 广播故障转移开始消息
     */
    private void broadcastFailoverStart(ClusterNode failedMaster, ClusterNode newMaster) {
        FailoverInfo info = new FailoverInfo(failedMaster.getNodeId(), newMaster.getNodeId());
        ClusterMessage message = ClusterMessage.create(
                ClusterMessage.MessageType.FAILOVER_START,
                clusterManager.getCurrentNode().getNodeId(),
                null,
                info);

        // 广播消息
        clusterManager.getNodes().values().forEach(node -> {
            if (node.getStatus() == ClusterNode.NodeStatus.ONLINE) {
                clusterManager.getClusterCommunicator().sendMessage(message, node);
            }
        });
    }

    /**
     * 将节点提升为主节点
     */
    private void promoteToMaster(ClusterNode node) {
        if (node == null) {
            logger.error("Cannot promote null node to master");
            return;
        }

        logger.info("Promoting node {} to master role", node.getNodeId());

        // 更改节点角色
        node.setRole(ClusterNode.NodeRole.MASTER);
        node.setMasterId(null); // 主节点没有master ID

        try {
            // 如果是本地节点，需要更新配置
            if (node.getNodeId().equals(clusterManager.getCurrentNode().getNodeId())) {
                // 通知所有从节点关于新主节点的信息
                ClusterMessage newMasterMsg = ClusterMessage.create(
                        ClusterMessage.MessageType.NODE_ADDED,
                        node.getNodeId(),
                        null,
                        node);

                clusterManager.getNodes().values().stream()
                        .filter(n -> n.getRole() == ClusterNode.NodeRole.SLAVE)
                        .forEach(slave -> {
                            clusterManager.getClusterCommunicator().sendMessage(newMasterMsg, slave);
                        });
            }
        } catch (Exception e) {
            logger.error("Error during node promotion to master", e);
        }
    }

    /**
     * 更新从节点的主节点信息
     */
    private void updateSlaves(ClusterNode failedMaster, ClusterNode newMaster) {
        clusterManager.getNodes().values().stream()
                .filter(node -> node.getRole() == ClusterNode.NodeRole.SLAVE)
                .filter(node -> failedMaster.getNodeId().equals(node.getMasterId()))
                .forEach(slave -> slave.setMasterId(newMaster.getNodeId()));
    }

    /**
     * 广播故障转移完成消息
     */
    private void broadcastFailoverEnd(ClusterNode newMaster) {
        ClusterMessage message = ClusterMessage.create(
                ClusterMessage.MessageType.FAILOVER_END,
                clusterManager.getCurrentNode().getNodeId(),
                null,
                newMaster.getNodeId());

        // 广播消息
        clusterManager.getNodes().values().forEach(node -> {
            if (node.getStatus() == ClusterNode.NodeStatus.ONLINE) {
                clusterManager.getClusterCommunicator().sendMessage(message, node);
            }
        });
    }

    /**
     * 故障转移信息类
     */
    private static class FailoverInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        private final String failedMasterId;
        private final String newMasterId;

        public FailoverInfo(String failedMasterId, String newMasterId) {
            this.failedMasterId = failedMasterId;
            this.newMasterId = newMasterId;
        }
    }
}