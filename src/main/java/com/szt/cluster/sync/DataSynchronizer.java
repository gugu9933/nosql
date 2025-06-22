package com.szt.cluster.sync;

import com.szt.cluster.communication.ClusterCommunicator;
import com.szt.cluster.node.ClusterNode;
import com.szt.config.RedisConfig;
import com.szt.core.db.RedisDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 数据同步器
 * 负责主从节点间的数据同步
 */
public class DataSynchronizer {
    private static final Logger logger = LoggerFactory.getLogger(DataSynchronizer.class);

    /**
     * 配置信息
     */
    private final RedisConfig config;

    /**
     * 集群通信器
     */
    private final ClusterCommunicator communicator;

    /**
     * 数据库列表
     */
    private final List<RedisDatabase> databases;

    /**
     * 主节点
     */
    private final ClusterNode masterNode;

    /**
     * 定时任务执行器
     */
    private final ScheduledExecutorService scheduler;

    /**
     * 同步状态
     */
    private volatile SyncState syncState = SyncState.IDLE;

    /**
     * 上次同步时间
     */
    private volatile long lastSyncTime = 0;

    /**
     * 同步端口（基于集群通信端口+1000）
     */
    private static final int SYNC_PORT_OFFSET = 11000;

    /**
     * 构造函数
     *
     * @param config       配置信息
     * @param communicator 集群通信器
     * @param databases    数据库列表
     * @param masterNode   主节点
     */
    public DataSynchronizer(RedisConfig config, ClusterCommunicator communicator,
            List<RedisDatabase> databases, ClusterNode masterNode) {
        this.config = config;
        this.communicator = communicator;
        this.databases = databases;
        this.masterNode = masterNode;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "data-sync-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动同步服务
     */
    public void start() {
        if (config.isMaster()) {
            // 主节点启动同步监听服务
            startSyncServer();
        } else {
            // 从节点定期同步数据
            scheduler.scheduleWithFixedDelay(
                    this::syncFromMaster,
                    0,
                    config.getSyncInterval(),
                    TimeUnit.SECONDS);
        }
    }

    /**
     * 主节点启动同步服务
     */
    private void startSyncServer() {
        Thread serverThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(config.getPort() + SYNC_PORT_OFFSET)) {
                logger.info("Data sync server started on port: {}", serverSocket.getLocalPort());
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        CompletableFuture.runAsync(() -> handleSyncRequest(clientSocket));
                    } catch (IOException e) {
                        if (!Thread.currentThread().isInterrupted()) {
                            logger.error("Error accepting sync connection", e);
                        }
                    }
                }
            } catch (IOException e) {
                logger.error("Error starting sync server", e);
            }
        });
        serverThread.setDaemon(true);
        serverThread.setName("sync-server-thread");
        serverThread.start();
    }

    /**
     * 处理同步请求（主节点端）
     */
    private void handleSyncRequest(Socket clientSocket) {
        try {
            // 设置超时
            clientSocket.setSoTimeout(10000);

            // 读取同步请求
            ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
            SyncRequest request = (SyncRequest) ois.readObject();

            logger.info("Received sync request from slave: {}", request.getNodeId());

            // 序列化数据库
            byte[] data = serializeDatabases();

            // 发送响应
            ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
            SyncResponse response = new SyncResponse(data, System.currentTimeMillis());
            oos.writeObject(response);
            oos.flush();

            logger.info("Sent {} bytes of data to slave: {}", data.length, request.getNodeId());
        } catch (Exception e) {
            logger.error("Error handling sync request", e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                logger.error("Error closing client socket", e);
            }
        }
    }

    /**
     * 停止同步服务
     */
    public void stop() {
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

    /**
     * 从主节点同步数据
     */
    private void syncFromMaster() {
        // 如果正在同步中，跳过本次同步
        if (syncState == SyncState.SYNCING) {
            logger.info("Sync already in progress, skipping");
            return;
        }

        syncState = SyncState.SYNCING;
        try {
            logger.info("Starting data sync from master: {}:{}", masterNode.getHost(), masterNode.getPort());

            // 创建到主节点的同步连接
            Socket socket = new Socket();
            socket.connect(
                    new java.net.InetSocketAddress(
                            masterNode.getHost(),
                            masterNode.getPort() + SYNC_PORT_OFFSET),
                    config.getSyncConnectTimeout());
            socket.setSoTimeout(config.getSyncReadTimeout());

            // 发送同步请求
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            SyncRequest request = new SyncRequest(config.getNodeId(), lastSyncTime);
            oos.writeObject(request);
            oos.flush();

            // 接收同步响应
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            SyncResponse response = (SyncResponse) ois.readObject();

            // 处理同步数据
            if (response.getData() != null && response.getData().length > 0) {
                deserializeDatabases(response.getData());
                lastSyncTime = response.getTimestamp();
                logger.info("Successfully synced {} bytes of data from master", response.getData().length);
            } else {
                logger.warn("Received empty sync data from master");
            }

            socket.close();
        } catch (Exception e) {
            logger.error("Failed to sync data from master", e);
        } finally {
            syncState = SyncState.IDLE;
        }
    }

    /**
     * 序列化数据库状态
     *
     * @return 序列化后的数据
     */
    private byte[] serializeDatabases() throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            // 写入数据库数量
            oos.writeInt(databases.size());

            // 逐个序列化数据库
            for (RedisDatabase db : databases) {
                oos.writeObject(db);
            }

            oos.flush();
            return baos.toByteArray();
        }
    }

    /**
     * 反序列化数据库状态
     *
     * @param data 序列化的数据
     */
    private void deserializeDatabases(byte[] data) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
                ObjectInputStream ois = new ObjectInputStream(bais)) {

            // 读取数据库数量
            int dbCount = ois.readInt();

            // 逐个反序列化并更新数据库
            for (int i = 0; i < dbCount && i < databases.size(); i++) {
                RedisDatabase syncedDb = (RedisDatabase) ois.readObject();
                // 设置事件发布器，因为它是transient的，不会被序列化
                syncedDb.setEventPublisher(databases.get(i).getEventPublisher());
                // 用同步的数据库替换本地数据库
                databases.set(i, syncedDb);
            }
        }
    }

    /**
     * 同步状态枚举
     */
    private enum SyncState {
        IDLE, // 空闲
        SYNCING // 同步中
    }

    /**
     * 同步请求类
     */
    private static class SyncRequest implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String nodeId;
        private final long lastSyncTime;

        public SyncRequest(String nodeId, long lastSyncTime) {
            this.nodeId = nodeId;
            this.lastSyncTime = lastSyncTime;
        }

        public String getNodeId() {
            return nodeId;
        }

        public long getLastSyncTime() {
            return lastSyncTime;
        }
    }

    /**
     * 同步响应类
     */
    private static class SyncResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        private final byte[] data;
        private final long timestamp;

        public SyncResponse(byte[] data, long timestamp) {
            this.data = data;
            this.timestamp = timestamp;
        }

        public byte[] getData() {
            return data;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}