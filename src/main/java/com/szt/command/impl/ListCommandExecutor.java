package com.szt.command.impl;

import com.redis.command.CommandExecutor;
import com.redis.common.exception.RedisCommandException;
import com.redis.core.data.RedisDataType;
import com.redis.core.data.RedisObject;
import com.redis.core.db.RedisDatabase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 列表命令执行器
 */
public class ListCommandExecutor implements CommandExecutor {

    private static final Set<String> SUPPORTED_COMMANDS = new HashSet<>(Arrays.asList(
            "lpush", "rpush", "lpop", "rpop", "llen", "lrange", "lindex", "lset", "lrem"));

    /**
     * 当前数据库
     */
    private final RedisDatabase database;

    /**
     * 构造函数
     */
    public ListCommandExecutor(RedisDatabase database) {
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
            case "lpush":
                return executeLPush(args);
            case "rpush":
                return executeRPush(args);
            case "lpop":
                return executeLPop(args);
            case "rpop":
                return executeRPop(args);
            case "llen":
                return executeLLen(args);
            case "lrange":
                return executeLRange(args);
            case "lindex":
                return executeLIndex(args);
            case "lset":
                return executeLSet(args);
            case "lrem":
                return executeLRem(args);
            default:
                throw new RedisCommandException("不支持的命令: " + cmdName);
        }
    }

    /**
     * 执行LPUSH命令
     */
    private String executeLPush(String[] args) {
        if (args == null || args.length < 2) {
            throw new RedisCommandException("wrong number of arguments for 'lpush' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);
        List<String> list;

        if (obj == null) {
            // 如果键不存在，创建新列表
            obj = RedisObject.createList();
            database.set(key, obj);
        } else if (obj.getType() != RedisDataType.LIST) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        list = obj.getTypedData();

        // 添加元素到列表头部
        for (int i = 1; i < args.length; i++) {
            list.add(0, args[i]);
        }

        return ":" + list.size() + "\r\n";
    }

    /**
     * 执行RPUSH命令
     */
    private String executeRPush(String[] args) {
        if (args == null || args.length < 2) {
            throw new RedisCommandException("wrong number of arguments for 'rpush' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);
        List<String> list;

        if (obj == null) {
            // 如果键不存在，创建新列表
            obj = RedisObject.createList();
            database.set(key, obj);
        } else if (obj.getType() != RedisDataType.LIST) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        list = obj.getTypedData();

        // 添加元素到列表尾部
        list.addAll(Arrays.asList(args).subList(1, args.length));

        return ":" + list.size() + "\r\n";
    }

    /**
     * 执行LPOP命令
     */
    private String executeLPop(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'lpop' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "$-1\r\n";
        }

        if (obj.getType() != RedisDataType.LIST) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        List<String> list = obj.getTypedData();
        if (list.isEmpty()) {
            return "$-1\r\n";
        }

        String value = list.remove(0);
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }

    /**
     * 执行RPOP命令
     */
    private String executeRPop(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'rpop' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "$-1\r\n";
        }

        if (obj.getType() != RedisDataType.LIST) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        List<String> list = obj.getTypedData();
        if (list.isEmpty()) {
            return "$-1\r\n";
        }

        String value = list.remove(list.size() - 1);
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }

    /**
     * 执行LLEN命令
     */
    private String executeLLen(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'llen' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return ":0\r\n";
        }

        if (obj.getType() != RedisDataType.LIST) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        List<String> list = obj.getTypedData();
        return ":" + list.size() + "\r\n";
    }

    /**
     * 执行LRANGE命令
     */
    private String executeLRange(String[] args) {
        if (args == null || args.length != 3) {
            throw new RedisCommandException("wrong number of arguments for 'lrange' command");
        }

        String key = args[0];
        int start, stop;

        try {
            start = Integer.parseInt(args[1]);
            stop = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not an integer or out of range");
        }

        RedisObject obj = database.get(key);
        if (obj == null) {
            return "*0\r\n";
        }

        if (obj.getType() != RedisDataType.LIST) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        List<String> list = obj.getTypedData();
        int size = list.size();

        // 处理负索引
        if (start < 0)
            start = size + start;
        if (stop < 0)
            stop = size + stop;

        // 限制范围
        start = Math.max(0, start);
        stop = Math.min(size - 1, stop);

        if (start > stop || start >= size) {
            return "*0\r\n";
        }

        // 构建返回结果
        StringBuilder result = new StringBuilder();
        int count = stop - start + 1;
        result.append("*").append(count).append("\r\n");

        for (int i = start; i <= stop; i++) {
            String value = list.get(i);
            result.append("$").append(value.length()).append("\r\n");
            result.append(value).append("\r\n");
        }

        return result.toString();
    }

    /**
     * 执行LINDEX命令
     */
    private String executeLIndex(String[] args) {
        if (args == null || args.length != 2) {
            throw new RedisCommandException("wrong number of arguments for 'lindex' command");
        }

        String key = args[0];
        int index;

        try {
            index = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not an integer or out of range");
        }

        RedisObject obj = database.get(key);
        if (obj == null) {
            return "$-1\r\n";
        }

        if (obj.getType() != RedisDataType.LIST) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        List<String> list = obj.getTypedData();
        if (index < 0) {
            index = list.size() + index;
        }

        if (index < 0 || index >= list.size()) {
            return "$-1\r\n";
        }

        String value = list.get(index);
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }

    /**
     * 执行LSET命令
     */
    private String executeLSet(String[] args) {
        if (args == null || args.length != 3) {
            throw new RedisCommandException("wrong number of arguments for 'lset' command");
        }

        String key = args[0];
        int index;
        String value = args[2];

        try {
            index = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not an integer or out of range");
        }

        RedisObject obj = database.get(key);
        if (obj == null) {
            throw new RedisCommandException("no such key");
        }

        if (obj.getType() != RedisDataType.LIST) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        List<String> list = obj.getTypedData();
        if (index < 0) {
            index = list.size() + index;
        }

        if (index < 0 || index >= list.size()) {
            throw new RedisCommandException("index out of range");
        }

        list.set(index, value);
        return "+OK\r\n";
    }

    /**
     * 执行LREM命令
     */
    private String executeLRem(String[] args) {
        if (args == null || args.length != 3) {
            throw new RedisCommandException("wrong number of arguments for 'lrem' command");
        }

        String key = args[0];
        int count;
        String value = args[2];

        try {
            count = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not an integer or out of range");
        }

        RedisObject obj = database.get(key);
        if (obj == null) {
            return ":0\r\n";
        }

        if (obj.getType() != RedisDataType.LIST) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        List<String> list = obj.getTypedData();
        int removed = 0;

        if (count > 0) {
            // 从头到尾删除count个
            for (int i = 0; i < list.size() && removed < count; i++) {
                if (list.get(i).equals(value)) {
                    list.remove(i);
                    removed++;
                    i--;
                }
            }
        } else if (count < 0) {
            // 从尾到头删除count个
            for (int i = list.size() - 1; i >= 0 && removed < -count; i--) {
                if (list.get(i).equals(value)) {
                    list.remove(i);
                    removed++;
                }
            }
        } else {
            // 删除所有
            for (int i = list.size() - 1; i >= 0; i--) {
                if (list.get(i).equals(value)) {
                    list.remove(i);
                    removed++;
                }
            }
        }

        return ":" + removed + "\r\n";
    }
}