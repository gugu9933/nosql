package com.szt.core.network;

import com.szt.command.CommandExecutor;
import com.szt.config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Redis网络服务器
 * 负责处理客户端连接和命令请求
 */
public class RedisNetworkServer {
    private static final Logger logger = LoggerFactory.getLogger(RedisNetworkServer.class);

    /**
     * 配置信息
     */
    private final RedisConfig config;

    /**
     * 命令执行器
     */
    private final CommandExecutor commandExecutor;

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
     * 构造函数
     *
     * @param config          配置信息
     * @param commandExecutor 命令执行器
     */
    public RedisNetworkServer(RedisConfig config, CommandExecutor commandExecutor) {
        this.config = config;
        this.commandExecutor = commandExecutor;
        this.executor = Executors.newCachedThreadPool();
    }

    /**
     * 启动网络服务
     */
    public void start() {
        try {
            serverSocket = new ServerSocket(config.getPort());
            running = true;

            logger.info("Redis server listening on port {}", config.getPort());

            // 启动接收连接线程
            executor.submit(this::acceptConnections);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start network server", e);
        }
    }

    /**
     * 停止网络服务
     */
    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            logger.error("Error closing server socket", e);
        }

        executor.shutdown();
    }

    /**
     * 接收客户端连接
     */
    private void acceptConnections() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                executor.submit(() -> handleClient(clientSocket));
            } catch (IOException e) {
                if (running) {
                    logger.error("Error accepting client connection", e);
                }
            }
        }
    }

    /**
     * 处理客户端连接
     *
     * @param clientSocket 客户端Socket
     */
    private void handleClient(Socket clientSocket) {
        try {
            // 创建客户端处理器
            ClientHandler handler = new ClientHandler(clientSocket, commandExecutor);
            handler.handle();
        } catch (Exception e) {
            logger.error("Error handling client", e);
        }
    }
}