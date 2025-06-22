package com.szt.command.impl;

import com.redis.command.CommandExecutor;
import com.redis.common.exception.RedisCommandException;
import com.redis.core.data.RedisObject;
import com.redis.core.db.RedisDatabase;

import java.util.*;

/**
 * 键操作命令执行器
 */
public class KeyCommandExecutor implements CommandExecutor {

    private static final Set<String> SUPPORTED_COMMANDS = new HashSet<>(Arrays.asList(
            "del", "exists", "type", "expire", "ttl", "persist", "keys"));

    /**
     * 当前数据库
     */
    private final RedisDatabase database;

    /**
     * 构造函数
     *
     * @param database 数据库
     */
    public KeyCommandExecutor(RedisDatabase database) {
        this.database = database;
    }

    @Override
    public Set<String> getSupportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    @Override
    public String execute(String command, String[] args) {
        String cmdName = command.toLowerCase();
        switch (cmdName) {
            case "del":
                return executeDel(args);
            case "exists":
                return executeExists(args);
            case "type":
                return executeType(args);
            case "expire":
                return executeExpire(args);
            case "ttl":
                return executeTtl(args);
            case "persist":
                return executePersist(args);
            case "keys":
                return executeKeys(args);
            default:
                throw new RedisCommandException("Unsupported command: " + cmdName);
        }
    }

    /**
     * 执行DEL命令
     */
    private String executeDel(String[] args) {
        if (args == null || args.length < 1) {
            throw new RedisCommandException("wrong number of arguments for 'del' command");
        }

        for (String key : args) {
            database.delete(key);
        }
        return "+OK\r\n";
    }

    /**
     * 执行EXISTS命令
     */
    private String executeExists(String[] args) {
        if (args == null || args.length < 1) {
            throw new RedisCommandException("wrong number of arguments for 'exists' command");
        }

        int count = 0;
        for (String key : args) {
            if (database.exists(key)) {
                count++;
            }
        }

        return ":" + count + "\r\n";
    }

    /**
     * 执行TYPE命令
     */
    private String executeType(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'type' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "+none\r\n";
        }

        return "+" + obj.getType().toString().toLowerCase() + "\r\n";
    }

    /**
     * 执行EXPIRE命令
     */
    private String executeExpire(String[] args) {
        if (args == null || args.length != 2) {
            throw new RedisCommandException("wrong number of arguments for 'expire' command");
        }

        String key = args[0];
        long seconds;
        try {
            seconds = Long.parseLong(args[1]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not an integer or out of range");
        }

        if (seconds <= 0) {
            // 如果过期时间小于等于0，删除键
            database.delete(key);
            return "+OK\r\n";
        }

        database.expire(key, seconds * 1000);
        return "+OK\r\n";
    }

    /**
     * 执行TTL命令
     */
    private String executeTtl(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'ttl' command");
        }

        String key = args[0];
        long ttlMillis = database.ttl(key);

        // 将毫秒转换为秒，向上取整
        long ttlSeconds = (ttlMillis + 999) / 1000; // 向上取整

        // 特殊情况保持原值
        if (ttlMillis == -1 || ttlMillis == -2) {
            ttlSeconds = ttlMillis;
        }

        // 使用Redis整数回复格式
        return ":" + ttlSeconds + "\r\n";
    }

    /**
     * 执行PERSIST命令
     */
    private String executePersist(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'persist' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);
        if (obj == null || obj.getExpireTime() == null) {
            return "+OK\r\n";
        }

        obj.setExpireTime(null);
        return "+OK\r\n";
    }

    /**
     * 执行KEYS命令
     */
    private String executeKeys(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'keys' command");
        }

        String pattern = args[0];
        Set<String> keys = database.getKeys();
        List<String> matchedKeys = new ArrayList<>();

        // 实现简单的模式匹配
        for (String key : keys) {
            if (matchPattern(key, pattern)) {
                matchedKeys.add(key);
            }
        }

        // 构建Redis协议格式的数组响应
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(matchedKeys.size()).append("\r\n");
        for (String key : matchedKeys) {
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
}