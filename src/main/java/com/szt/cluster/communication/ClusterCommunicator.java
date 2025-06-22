package com.szt.cluster.communication;

import com.szt.cluster.node.ClusterNode;
import com.szt.config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * 集群通信器
 * 负责节点间的消息收发
 */
public class ClusterCommunicator {
    private static final Logger logger = LoggerFactory.getLogger(ClusterCommunicator.class);

    /**
     * 配置信息
     */
    private final RedisConfig config;

    /**
     * 消息处理器
     */
    private final Consumer<ClusterMessage> messageHandler;

    /**
     * 服务器Socket
     */
    private ServerSocket serverSocket;

    /**
     * 线程池
     */
    private final ExecutorService executor;

    /**
     * 是否正在运行
     */
    private volatile boolean running;

    /**
     * 连接超时时间（毫秒）
     */
    private static final int CONNECTION_TIMEOUT = 3000;

    /**
     * 最大重试次数
     */
    private static final int MAX_RETRIES = 3;

    /**
     * 集群通信端口偏移量
     */
    private static final int CLUSTER_PORT_OFFSET = 20000;

    /**
     * 是否打印连接错误
     */
    private volatile boolean logConnectionErrors = false;

    /**
     * 构造函数
     *
     * @param config         配置信息
     * @param messageHandler 消息处理器
     */
    public ClusterCommunicator(RedisConfig config, Consumer<ClusterMessage> messageHandler) {
        this.config = config;
        this.messageHandler = messageHandler;
        this.executor = Executors.newCachedThreadPool();
    }

    /**
     * 启动通信服务
     */
    public void start() {
        try {
            // 创建服务器Socket，使用固定偏移量计算集群通信端口
            int clusterPort = config.getPort() + CLUSTER_PORT_OFFSET;
            serverSocket = new ServerSocket(clusterPort);
            running = true;

            // 启动消息接收线程
            executor.submit(this::receiveMessages);

            // 不输出启动日志，避免日志污染
        } catch (IOException e) {
            // 静默处理启动异常
        }
    }

    /**
     * 停止通信服务
     */
    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            // 静默处理关闭异常
        }

        executor.shutdown();
    }

    /**
     * 获取节点的集群通信端口
     * 
     * @param node 目标节点
     * @return 集群通信端口
     */
    private int getClusterPort(ClusterNode node) {
        return node.getPort() + CLUSTER_PORT_OFFSET;
    }

    /**
     * 发送消息到指定节点
     *
     * @param message 消息
     * @param node    目标节点
     */
    public void sendMessage(ClusterMessage message, ClusterNode node) {
        executor.submit(() -> {
            int retries = 0;
            boolean sent = false;
            int targetPort = getClusterPort(node);

            while (!sent && retries < MAX_RETRIES) {
                Socket socket = null;
                try {
                    // 使用InetSocketAddress并设置超时
                    socket = new Socket();
                    socket.connect(new InetSocketAddress(node.getHost(), targetPort), CONNECTION_TIMEOUT);

                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(message);
                    out.flush();
                    sent = true;
                } catch (IOException e) {
                    retries++;
                    try {
                        Thread.sleep(500); // 重试前等待500毫秒
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                } finally {
                    if (socket != null) {
                        try {
                            socket.close();
                        } catch (IOException e) {
                            // 静默处理关闭错误
                        }
                    }
                }
            }
        });
    }

    /**
     * 接收消息循环
     */
    private void receiveMessages() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                executor.submit(() -> handleClientConnection(clientSocket));
            } catch (IOException e) {
                // 静默处理接收异常
            }
        }
    }

    /**
     * 处理客户端连接
     *
     * @param clientSocket 客户端Socket
     */
    private void handleClientConnection(Socket clientSocket) {
        try (ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream())) {
            ClusterMessage message = (ClusterMessage) in.readObject();
            messageHandler.accept(message);
        } catch (IOException | ClassNotFoundException e) {
            // 静默处理连接异常
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                // 静默处理关闭错误
            }
        }
    }

    /**
     * 设置是否记录连接错误
     */
    public void setLogConnectionErrors(boolean logErrors) {
        this.logConnectionErrors = logErrors;
    }
}