package com.szt.cluster.node;

import lombok.Data;

import java.io.Serializable;
import java.time.Instant;

/**
 * 集群节点类
 * 表示集群中的一个节点实例
 */
@Data
public class ClusterNode implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 节点ID
     */
    private String nodeId;

    /**
     * 节点角色(master/slave)
     */
    private NodeRole role;

    /**
     * 节点主机地址
     */
    private String host;

    /**
     * 节点端口
     */
    private int port;

    /**
     * 如果是从节点,关联的主节点ID
     */
    private String masterId;

    /**
     * 节点状态
     */
    private NodeStatus status;

    /**
     * 最后一次心跳时间
     */
    private Instant lastHeartbeat;

    /**
     * 节点角色枚举
     */
    public enum NodeRole {
        MASTER,
        SLAVE
    }

    /**
     * 节点状态枚举
     */
    public enum NodeStatus {
        ONLINE, // 在线
        OFFLINE, // 离线
        SUSPECT, // 可疑(可能下线)
        HANDSHAKE // 握手中
    }

    /**
     * 更新心跳时间
     */
    public void updateHeartbeat() {
        this.lastHeartbeat = Instant.now();
    }

    /**
     * 检查节点是否存活
     * 
     * @param timeoutSeconds 超时时间(秒)
     * @return 是否存活
     */
    public boolean isAlive(long timeoutSeconds) {
        if (lastHeartbeat == null) {
            return false;
        }
        return Instant.now().minusSeconds(timeoutSeconds).isBefore(lastHeartbeat);
    }
}