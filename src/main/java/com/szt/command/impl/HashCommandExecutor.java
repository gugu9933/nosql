package com.szt.command.impl;

import com.redis.command.CommandExecutor;
import com.redis.common.exception.RedisCommandException;
import com.redis.core.data.RedisDataType;
import com.redis.core.data.RedisObject;
import com.redis.core.db.RedisDatabase;

import java.util.*;

/**
 * 哈希命令执行器
 */
public class HashCommandExecutor implements CommandExecutor {

    private static final Set<String> SUPPORTED_COMMANDS = new HashSet<>(Arrays.asList(
            "hset", "hget", "hdel", "hexists", "hgetall", "hkeys", "hvals",
            "hlen", "hmget", "hmset", "hsetnx", "hincrby"));

    /**
     * 当前数据库
     */
    private final RedisDatabase database;

    /**
     * 构造函数
     */
    public HashCommandExecutor(RedisDatabase database) {
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
            case "hset":
                return executeHSet(args);
            case "hget":
                return executeHGet(args);
            case "hdel":
                return executeHDel(args);
            case "hexists":
                return executeHExists(args);
            case "hgetall":
                return executeHGetAll(args);
            case "hkeys":
                return executeHKeys(args);
            case "hvals":
                return executeHVals(args);
            case "hlen":
                return executeHLen(args);
            case "hmget":
                return executeHMGet(args);
            case "hmset":
                return executeHMSet(args);
            case "hsetnx":
                return executeHSetNx(args);
            case "hincrby":
                return executeHIncrBy(args);
            default:
                throw new RedisCommandException("不支持的命令: " + cmdName);
        }
    }

    /**
     * 执行HSET命令
     */
    private String executeHSet(String[] args) {
        if (args == null || args.length < 3 || args.length % 2 != 1) {
            throw new RedisCommandException("wrong number of arguments for 'hset' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);
        Map<String, String> hash;

        if (obj == null) {
            // 创建新的哈希对象
            obj = RedisObject.createHash();
            database.set(key, obj);
        } else if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        hash = obj.getTypedData();

        int added = 0;
        for (int i = 1; i < args.length; i += 2) {
            String field = args[i];
            String value = args[i + 1];
            if (hash.put(field, value) == null) {
                added++;
            }
        }

        return String.valueOf(added);
    }

    /**
     * 执行HGET命令
     */
    private String executeHGet(String[] args) {
        if (args == null || args.length != 2) {
            throw new RedisCommandException("wrong number of arguments for 'hget' command");
        }

        String key = args[0];
        String field = args[1];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return null;
        }

        if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Map<String, String> hash = obj.getTypedData();
        return hash.get(field);
    }

    /**
     * 执行HDEL命令
     */
    private String executeHDel(String[] args) {
        if (args == null || args.length < 2) {
            throw new RedisCommandException("wrong number of arguments for 'hdel' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "0";
        }

        if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Map<String, String> hash = obj.getTypedData();
        int deleted = 0;

        for (int i = 1; i < args.length; i++) {
            if (hash.remove(args[i]) != null) {
                deleted++;
            }
        }

        return String.valueOf(deleted);
    }

    /**
     * 执行HEXISTS命令
     */
    private String executeHExists(String[] args) {
        if (args == null || args.length != 2) {
            throw new RedisCommandException("wrong number of arguments for 'hexists' command");
        }

        String key = args[0];
        String field = args[1];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "0";
        }

        if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Map<String, String> hash = obj.getTypedData();
        return hash.containsKey(field) ? "1" : "0";
    }

    /**
     * 执行HGETALL命令
     */
    private String executeHGetAll(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'hgetall' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "[]";
        }

        if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Map<String, String> hash = obj.getTypedData();
        List<String> result = new ArrayList<>();

        for (Map.Entry<String, String> entry : hash.entrySet()) {
            result.add(entry.getKey());
            result.add(entry.getValue());
        }

        return result.toString();
    }

    /**
     * 执行HKEYS命令
     */
    private String executeHKeys(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'hkeys' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "[]";
        }

        if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Map<String, String> hash = obj.getTypedData();
        return new ArrayList<>(hash.keySet()).toString();
    }

    /**
     * 执行HLEN命令
     */
    private String executeHLen(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'hlen' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "0";
        }

        if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Map<String, String> hash = obj.getTypedData();
        return String.valueOf(hash.size());
    }

    /**
     * 执行HMGET命令
     */
    private String executeHMGet(String[] args) {
        if (args == null || args.length < 2) {
            throw new RedisCommandException("wrong number of arguments for 'hmget' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);
        List<String> result = new ArrayList<>();

        if (obj == null) {
            // 键不存在，返回所有null
            for (int i = 0; i < args.length - 1; i++) {
                result.add(null);
            }
            return result.toString();
        }

        if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Map<String, String> hash = obj.getTypedData();

        // 获取指定字段的值
        for (int i = 1; i < args.length; i++) {
            result.add(hash.get(args[i]));
        }

        return result.toString();
    }

    /**
     * 执行HMSET命令
     */
    private String executeHMSet(String[] args) {
        if (args == null || args.length < 3 || args.length % 2 != 1) {
            throw new RedisCommandException("wrong number of arguments for 'hmset' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);
        Map<String, String> hash;

        if (obj == null) {
            // 如果键不存在，创建新哈希
            obj = RedisObject.createHash();
            database.set(key, obj);
        } else if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        hash = obj.getTypedData();

        // 设置字段值
        for (int i = 1; i < args.length; i += 2) {
            hash.put(args[i], args[i + 1]);
        }

        return "OK";
    }

    /**
     * 执行HSETNX命令
     */
    private String executeHSetNx(String[] args) {
        if (args == null || args.length != 3) {
            throw new RedisCommandException("wrong number of arguments for 'hsetnx' command");
        }

        String key = args[0];
        String field = args[1];
        String value = args[2];
        RedisObject obj = database.get(key);
        Map<String, String> hash;

        if (obj == null) {
            // 如果键不存在，创建新哈希
            obj = RedisObject.createHash();
            database.set(key, obj);
        } else if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        hash = obj.getTypedData();

        // 如果字段不存在，则设置值并返回1
        if (!hash.containsKey(field)) {
            hash.put(field, value);
            return "1";
        }

        // 字段已存在，返回0
        return "0";
    }

    /**
     * 执行HVALS命令
     */
    private String executeHVals(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'hvals' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "[]";
        }

        if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Map<String, String> hash = obj.getTypedData();
        return new ArrayList<>(hash.values()).toString();
    }

    /**
     * 执行HINCRBY命令
     */
    private String executeHIncrBy(String[] args) {
        if (args == null || args.length != 3) {
            throw new RedisCommandException("wrong number of arguments for 'hincrby' command");
        }

        String key = args[0];
        String field = args[1];
        long increment;

        try {
            increment = Long.parseLong(args[2]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not an integer or out of range");
        }

        RedisObject obj = database.get(key);
        Map<String, String> hash;

        if (obj == null) {
            // 如果键不存在，创建新哈希
            obj = RedisObject.createHash();
            database.set(key, obj);
        } else if (obj.getType() != RedisDataType.HASH) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        hash = obj.getTypedData();

        // 获取当前值
        long currentValue = 0;
        String currentStr = hash.get(field);
        if (currentStr != null) {
            try {
                currentValue = Long.parseLong(currentStr);
            } catch (NumberFormatException e) {
                throw new RedisCommandException("hash value is not an integer");
            }
        }

        // 增加值
        long newValue = currentValue + increment;
        hash.put(field, String.valueOf(newValue));

        return String.valueOf(newValue);
    }
}