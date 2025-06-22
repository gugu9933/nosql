package com.szt;

import com.szt.config.ConfigLoader;
import com.szt.config.RedisConfig;
import com.szt.core.server.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis服务器启动类
 */
public class RedisServerStarter {

    private static final Logger logger = LoggerFactory.getLogger(RedisServerStarter.class);

    /**
     * 主方法
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        try {
            // 解析命令行参数
            parseArgs(args);

            // 加载配置
            RedisConfig config = ConfigLoader.loadConfig();

            // 打印欢迎信息
            printWelcomeInfo(config);

            // 启动服务器
            RedisServer.start(config);

            // 注册关闭钩子
            registerShutdownHook();

            logger.info("Redis server started successfully");
        } catch (Exception e) {
            logger.error("Failed to start Redis server", e);
            System.exit(1);
        }
    }

    /**
     * 解析命令行参数
     *
     * @param args 命令行参数
     */
    private static void parseArgs(String[] args) {
        if (args == null || args.length == 0) {
            return;
        }

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                String key = arg.substring(2);
                String value = null;
                if (key.contains("=")) {
                    String[] parts = key.split("=", 2);
                    key = parts[0];
                    value = parts[1];
                } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    value = args[++i];
                }

                if (value != null) {
                    System.setProperty(key, value);
                }
            }
        }
    }

    /**
     * 打印欢迎信息
     *
     * @param config 配置信息
     */
    private static void printWelcomeInfo(RedisConfig config) {
        logger.info("Starting Java-Redis Server v1.0.0");
        logger.info("OS: {}", System.getProperty("os.name"));
        logger.info("Java Version: {}", System.getProperty("java.version"));
        logger.info("Server Mode: {}", config.isMaster() ? "Master" : "Slave");
        logger.info("Listening on {}:{}", config.getHost(), config.getPort());
        logger.info("Database Count: {}", config.getDatabaseCount());
        logger.info("Persistence Mode: {}", config.getPersistenceMode());
    }

    /**
     * 注册关闭钩子
     */
    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Redis server...");
            try {
                RedisServer.stop();
                logger.info("Redis server stopped successfully");
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));
    }
}