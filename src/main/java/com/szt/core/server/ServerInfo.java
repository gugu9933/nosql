package com.szt.core.server;

import com.szt.core.data.RedisObject;
import com.szt.event.KeyEvent;
import com.szt.event.KeyEventListener;
import lombok.Data;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务器信息类
 * 记录服务器运行状态和统计信息
 */
@Data
public class ServerInfo implements KeyEventListener, Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 操作系统名称
     */
    private final String osName;

    /**
     * 服务器启动时间（毫秒）
     */
    private final long startTimeMillis;

    /**
     * 活跃客户端数量
     */
    private final AtomicInteger clientCount;

    /**
     * 内存占用（字节）
     */
    private final AtomicInteger memoryUsage;

    /**
     * 命令执行总数
     */
    private final AtomicInteger commandCount;

    /**
     * 键空间命中次数
     */
    private final AtomicInteger keyspaceHits;

    /**
     * 键空间未命中次数
     */
    private final AtomicInteger keyspaceMisses;

    public ServerInfo() {
        this.osName = System.getProperty("os.name");
        this.startTimeMillis = System.currentTimeMillis();
        this.clientCount = new AtomicInteger(0);
        this.memoryUsage = new AtomicInteger(0);
        this.commandCount = new AtomicInteger(0);
        this.keyspaceHits = new AtomicInteger(0);
        this.keyspaceMisses = new AtomicInteger(0);
    }

    /**
     * 获取服务器运行时间（秒）
     */
    public long getUptimeInSeconds() {
        return (System.currentTimeMillis() - startTimeMillis) / 1000;
    }

    /**
     * 增加命令执行次数
     */
    public void incrementCommandCount() {
        commandCount.incrementAndGet();
    }

    /**
     * 客户端连接
     */
    public void clientConnected() {
        clientCount.incrementAndGet();
    }

    /**
     * 客户端断开连接
     */
    public void clientDisconnected() {
        clientCount.decrementAndGet();
    }

    @Override
    public void onEvent(KeyEvent event) {
        // 更新内存使用统计
        updateMemoryUsage(event);

        // 更新键空间统计
        switch (event.getType()) {
            case ADD:
            case UPDATE:
                keyspaceHits.incrementAndGet();
                break;
            default:
                break;
        }
    }

    /**
     * 更新内存使用统计
     */
    private void updateMemoryUsage(KeyEvent event) {
        RedisObject value = event.getValue();
        if (value == null) {
            return;
        }

        // 这里只是一个简单的估算，实际内存使用需要更复杂的计算
        Object data = value.getData();
        int estimatedSize = 0;

        if (data instanceof String) {
            estimatedSize = ((String) data).length() * 2; // 假设每个字符占2字节
        } else if (data instanceof byte[]) {
            estimatedSize = ((byte[]) data).length;
        }

        memoryUsage.addAndGet(estimatedSize);
    }
}