package com.szt.command;

import com.redis.command.impl.*;
import com.redis.common.exception.RedisCommandException;
import com.redis.common.exception.RedisException;
import com.redis.core.data.RedisObject;
import com.redis.core.db.DatabaseManager;
import com.redis.core.db.RedisDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 默认命令执行器实现
 */
public class DefaultCommandExecutor implements CommandExecutor {
    private static final Logger logger = LoggerFactory.getLogger(DefaultCommandExecutor.class);

    /**
     * 数据库管理器
     */
    private final DatabaseManager databaseManager;

    /**
     * 是否启用集群模式
     */
    private final boolean clusterEnabled;

    /**
     * 当前数据库索引
     */
    private int currentDbIndex = 0;

    /**
     * 命令执行器列表
     */
    private final List<CommandExecutor> commandExecutors = new ArrayList<>();

    /**
     * 构造函数
     *
     * @param databaseManager 数据库管理器
     * @param clusterEnabled  是否启用集群模式
     */
    public DefaultCommandExecutor(DatabaseManager databaseManager, boolean clusterEnabled) {
        this.databaseManager = databaseManager;
        this.clusterEnabled = clusterEnabled;

        // 初始化当前数据库的命令执行器
        initCommandExecutors();
    }

    /**
     * 初始化命令执行器
     */
    private void initCommandExecutors() {
        RedisDatabase currentDb = databaseManager.getDatabase(currentDbIndex);

        // 注册各种数据类型的命令执行器
        commandExecutors.add(new KeyCommandExecutor(currentDb));
        commandExecutors.add(new StringCommandExecutor(currentDb));
        commandExecutors.add(new ListCommandExecutor(currentDb));
        commandExecutors.add(new SetCommandExecutor(currentDb));
        commandExecutors.add(new HashCommandExecutor(currentDb));
        commandExecutors.add(new ZSetCommandExecutor(currentDb));
    }

    @Override
    public Set<String> getSupportedCommands() {
        Set<String> commands = new HashSet<>();

        // 添加通用命令
        commands.add("ping");
        commands.add("echo");
        commands.add("del");
        commands.add("exists");
        commands.add("keys");
        commands.add("flushdb");
        commands.add("info");
        commands.add("select");

        // 添加所有数据类型命令执行器支持的命令
        for (CommandExecutor executor : commandExecutors) {
            commands.addAll(executor.getSupportedCommands());
        }

        // 只在集群模式下添加集群相关命令
        if (clusterEnabled) {
            commands.add("readonly");
            commands.add("slaveof");
            commands.add("role");
        }

        return commands;
    }

    @Override
    public String execute(String command, String[] args) {
        try {
            logger.debug("Executing command: {}", command);

            // 检查命令是否为空
            if (command == null || command.trim().isEmpty()) {
                return "-ERR no command specified";
            }

            // 检查集群相关命令
            if (!clusterEnabled && isClusterCommand(command)) {
                return "-ERR This instance is not running in cluster mode";
            }

            // 特殊命令处理
            if ("SELECT".equalsIgnoreCase(command)) {
                return handleSelect(args);
            }

            // 获取当前数据库
            RedisDatabase db = databaseManager.getDatabase(currentDbIndex);

            // 根据命令类型执行不同的操作
            switch (command.toUpperCase()) {
                case "PING":
                    return "+PONG\r\n";
                case "ECHO":
                    return args.length > 0 ? "+" + args[0] + "\r\n" : "+\r\n";
                case "DEL":
                    return handleDel(db, args);
                case "EXISTS":
                    return handleExists(db, args);
                case "KEYS":
                    return handleKeys(db, args);
                case "FLUSHDB":
                    return handleFlushDb(db);
                case "INFO":
                    return handleInfo();
                case "READONLY":
                    return handleReadOnly();
            }

            // 尝试由数据类型命令执行器处理命令
            for (CommandExecutor executor : commandExecutors) {
                if (executor.getSupportedCommands().contains(command.toLowerCase())) {
                    String result = executor.execute(command, args);
                    // 如果结果是OK，确保返回Redis协议格式
                    if ("OK".equals(result)) {
                        return "+OK\r\n";
                    }
                    return result;
                }
            }

            return "-ERR unknown command '" + command + "'\r\n";
        } catch (RedisCommandException e) {
            // 直接传递命令执行异常
            logger.warn("Command execution error: {}", e.getMessage());
            return "-ERR " + e.getMessage() + "\r\n";
        } catch (RedisException e) {
            // 避免重复包装异常消息
            String msg = e.getMessage();
            if (msg != null && msg.startsWith("Command Error:")) {
                logger.error("Redis error: {}", msg);
                return "-ERR " + msg + "\r\n";
            } else {
                logger.error("Redis error: {}", msg);
                return "-ERR " + msg + "\r\n";
            }
        } catch (Exception e) {
            // 处理未预期的异常
            String msg = e.getMessage();
            logger.error("Unexpected error executing command: {}", command, e);
            return "-ERR internal error: " + (msg != null ? msg : "unknown error") + "\r\n";
        }
    }

    /**
     * 判断是否为集群相关命令
     */
    private boolean isClusterCommand(String command) {
        String cmd = command.toLowerCase();
        return cmd.equals("readonly") ||
                cmd.equals("slaveof") ||
                cmd.equals("role");
    }

    /**
     * 处理READONLY命令
     */
    private String handleReadOnly() {
        // 这个命令在从节点上设置连接为只读模式
        // 在我们的简化实现中，只需返回OK即可
        return "+OK\r\n";
    }

    /**
     * 处理SELECT命令
     */
    private String handleSelect(String[] args) {
        if (args.length != 1) {
            return "-ERR wrong number of arguments for 'select' command\r\n";
        }

        try {
            int index = Integer.parseInt(args[0]);
            if (index < 0 || index >= databaseManager.getDatabases().size()) {
                return "-ERR invalid DB index\r\n";
            }

            currentDbIndex = index;

            // 更新命令执行器的数据库引用
            initCommandExecutors();

            return "+OK\r\n";
        } catch (NumberFormatException e) {
            return "-ERR value is not an integer or out of range\r\n";
        }
    }

    /**
     * 处理DEL命令
     */
    private String handleDel(RedisDatabase db, String[] args) {
        if (args.length < 1) {
            return "-ERR wrong number of arguments for 'del' command\r\n";
        }

        int count = 0;
        for (String key : args) {
            RedisObject removed = db.delete(key);
            if (removed != null) {
                count++;
            }
        }

        return ":" + count + "\r\n";
    }

    /**
     * 处理EXISTS命令
     */
    private String handleExists(RedisDatabase db, String[] args) {
        if (args.length < 1) {
            return "-ERR wrong number of arguments for 'exists' command\r\n";
        }

        int count = 0;
        for (String key : args) {
            if (db.exists(key)) {
                count++;
            }
        }

        return ":" + count + "\r\n";
    }

    /**
     * 处理KEYS命令
     */
    private String handleKeys(RedisDatabase db, String[] args) {
        if (args.length != 1) {
            return "-ERR wrong number of arguments for 'keys' command\r\n";
        }

        String pattern = args[0];
        Set<String> keys = db.getKeys();

        // 简单实现，只支持*通配符
        List<String> result = new ArrayList<>();
        if ("*".equals(pattern)) {
            result.addAll(keys);
        } else {
            // 实现简单的模式匹配
            for (String key : keys) {
                if (matchPattern(key, pattern)) {
                    result.add(key);
                }
            }
        }

        // 构建Redis协议格式的数组响应
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(result.size()).append("\r\n");
        for (String key : result) {
            sb.append("$").append(key.length()).append("\r\n");
            sb.append(key).append("\r\n");
        }
        return sb.toString();
    }

    /**
     * 简单的模式匹配实现
     */
    private boolean matchPattern(String key, String pattern) {
        // 将Redis模式转换为Java正则表达式
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return key.matches(regex);
    }

    /**
     * 处理FLUSHDB命令
     */
    private String handleFlushDb(RedisDatabase db) {
        db.clear();
        return "+OK\r\n";
    }

    /**
     * 处理INFO命令
     */
    private String handleInfo() {
        StringBuilder info = new StringBuilder();
        info.append("# Server\r\n");
        info.append("redis_version:").append("1.0.0").append("\r\n");
        info.append("os:").append(System.getProperty("os.name")).append("\r\n");
        info.append("process_id:").append(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]).append("\r\n");

        info.append("\r\n# Clients\r\n");
        info.append("connected_clients:").append("1").append("\r\n");

        info.append("\r\n# Memory\r\n");
        info.append("used_memory:").append(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
                .append("\r\n");
        info.append("used_memory_human:")
                .append(formatBytes(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()))
                .append("\r\n");

        info.append("\r\n# Persistence\r\n");
        info.append("loading:0\r\n");
        info.append("rdb_changes_since_last_save:0\r\n");

        info.append("\r\n# Stats\r\n");
        info.append("total_connections_received:1\r\n");
        info.append("total_commands_processed:0\r\n");

        info.append("\r\n# Replication\r\n");
        info.append("role:").append(clusterEnabled ? "slave" : "master").append("\r\n");
        info.append("connected_slaves:0\r\n");

        info.append("\r\n# Keyspace\r\n");
        for (int i = 0; i < databaseManager.getDatabases().size(); i++) {
            RedisDatabase db = databaseManager.getDatabase(i);
            int keys = db.size();
            if (keys > 0) {
                info.append("db").append(i).append(":keys=").append(keys).append("\r\n");
            }
        }

        String infoStr = info.toString();
        return "$" + infoStr.length() + "\r\n" + infoStr + "\r\n";
    }

    /**
     * 格式化字节数为人类可读格式
     */
    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + "B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2fKB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2fMB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.2fGB", bytes / (1024.0 * 1024 * 1024));
        }
    }
}