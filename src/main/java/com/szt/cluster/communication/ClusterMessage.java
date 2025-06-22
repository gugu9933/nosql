package com.szt.cluster.communication;

import lombok.Data;

import java.io.Serializable;
import java.time.Instant;

/**
 * 集群消息类
 * 用于节点间通信
 */
@Data
public class ClusterMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 消息类型
     */
    private MessageType type;

    /**
     * 发送者节点ID
     */
    private String senderId;

    /**
     * 接收者节点ID
     */
    private String receiverId;

    /**
     * 消息内容
     */
    private Object payload;

    /**
     * 消息时间戳
     */
    private Instant timestamp;

    /**
     * 消息类型枚举
     */
    public enum MessageType {
        HEARTBEAT, // 心跳
        PING, // Ping
        PONG, // Pong
        SYNC_REQUEST, // 同步请求
        SYNC_RESPONSE, // 同步响应
        NODE_ADDED, // 节点加入
        NODE_REMOVED, // 节点移除
        FAILOVER_START, // 故障转移开始
        FAILOVER_END // 故障转移结束
    }

    /**
     * 创建新消息
     *
     * @param type       消息类型
     * @param senderId   发送者ID
     * @param receiverId 接收者ID
     * @param payload    消息内容
     * @return 消息实例
     */
    public static ClusterMessage create(MessageType type, String senderId, String receiverId, Object payload) {
        ClusterMessage message = new ClusterMessage();
        message.setType(type);
        message.setSenderId(senderId);
        message.setReceiverId(receiverId);
        message.setPayload(payload);
        message.setTimestamp(Instant.now());
        return message;
    }
}