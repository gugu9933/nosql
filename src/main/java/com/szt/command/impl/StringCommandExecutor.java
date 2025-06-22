package com.szt.command.impl;

import com.szt.command.CommandExecutor;
import com.szt.command.RedisCommand;
import com.szt.common.exception.RedisCommandException;
import com.szt.common.exception.RedisException;
import com.szt.core.data.RedisDataType;
import com.szt.core.data.RedisObject;
import com.szt.core.db.RedisDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 字符串命令执行器
 */
public class StringCommandExecutor implements CommandExecutor {
    private static final Logger logger = LoggerFactory.getLogger(StringCommandExecutor.class);

    private static final Set<String> SUPPORTED_COMMANDS = new HashSet<>(Arrays.asList(
            "set", "get", "getset", "append", "strlen", "incr", "incrby",
            "decr", "decrby", "setex", "setnx", "mset", "mget"));

    /**
     * 当前数据库
     */
    private final RedisDatabase database;

    /**
     * 构造函数
     *
     * @param database 数据库
     */
    public StringCommandExecutor(RedisDatabase database) {
        this.database = database;
    }

    @Override
    public Set<String> getSupportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    @Override
    public String execute(String command, String[] args) {
        String cmdName = command.toLowerCase();
        logger.debug("执行命令: {} 参数: {}", cmdName, Arrays.toString(args));
        switch (cmdName) {
            case "set":
                return executeSet(args);
            case "get":
                return executeGet(args);
            case "getset":
                return executeGetSet(args);
            case "incr":
                return executeIncr(args);
            case "incrby":
                return executeIncrBy(args);
            case "decr":
                return executeDecr(args);
            case "decrby":
                return executeDecrBy(args);
            default:
                throw new RedisCommandException("Unsupported command: " + cmdName);
        }
    }

    public String execute(RedisCommand command) {
        String cmdName = command.getName().toLowerCase();
        logger.debug("执行命令对象: {} 参数: {}", cmdName, Arrays.toString(command.getArgs()));
        switch (cmdName) {
            case "set":
                return executeSet(command);
            case "get":
                return executeGet(command);
            case "getset":
                return executeGetSet(command);
            // 其他命令实现...
            default:
                throw new RedisCommandException("Unsupported command: " + cmdName);
        }
    }

    private String executeSet(String[] args) {
        if (args == null || args.length < 2) {
            throw new RedisCommandException("wrong number of arguments for 'set' command");
        }

        String key = args[0];
        String value = args[1];

        logger.debug("SET 原始值: '{}'", value);

        // 处理引号和确保完整的字符串值
        if (value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1);
        } else if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length() - 1);
        }

        // 检查是否有多个参数被错误分割 (由于空格)
        if (args.length > 2) {
            StringBuilder fullValue = new StringBuilder(value);
            for (int i = 2; i < args.length; i++) {
                fullValue.append(" ").append(args[i]);
            }
            value = fullValue.toString();

            // 如果完整值是被引号包围的，去掉首尾引号
            if ((value.startsWith("'") && value.endsWith("'")) ||
                    (value.startsWith("\"") && value.endsWith("\""))) {
                value = value.substring(1, value.length() - 1);
            }
        }

        logger.debug("SET 处理后的值: '{}'", value);

        if (key == null || key.isEmpty()) {
            throw new RedisCommandException("key cannot be null or empty");
        }
        if (value == null) {
            throw new RedisCommandException("value cannot be null");
        }

        try {
            RedisObject obj = RedisObject.createString(value);
            database.set(key, obj);
            return "+OK\r\n";
        } catch (RedisException e) {
            throw new RedisCommandException(e.getMessage() != null ? e.getMessage() : "internal error");
        }
    }

    private String executeGet(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'get' command");
        }

        String key = args[0];
        if (key == null) {
            throw new RedisCommandException("key cannot be null");
        }

        try {
            RedisObject obj = database.get(key);
            if (obj == null) {
                return "$-1\r\n";
            }

            if (obj.getType() != RedisDataType.STRING) {
                throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            String value = obj.getTypedData();
            logger.debug("GET 键: '{}', 获取的值: '{}'", key, value);

            return "$" + value.length() + "\r\n" + value + "\r\n";
        } catch (RedisCommandException e) {
            throw e;
        } catch (Exception e) {
            throw new RedisCommandException("internal error: " + e.getMessage());
        }
    }

    private String executeGetSet(String[] args) {
        if (args == null || args.length != 2) {
            throw new RedisCommandException("wrong number of arguments for 'getset' command");
        }

        String key = args[0];
        String newValue = args[1];

        logger.debug("GETSET 键: '{}', 新值: '{}'", key, newValue);

        // 处理引号
        if (newValue.startsWith("\"") && newValue.endsWith("\"")) {
            newValue = newValue.substring(1, newValue.length() - 1);
        } else if (newValue.startsWith("'") && newValue.endsWith("'")) {
            newValue = newValue.substring(1, newValue.length() - 1);
        }

        if (key == null || key.isEmpty()) {
            throw new RedisCommandException("key cannot be null or empty");
        }
        if (newValue == null) {
            throw new RedisCommandException("value cannot be null");
        }

        try {
            RedisObject oldObj = database.get(key);
            String oldValue = null;

            if (oldObj != null) {
                if (oldObj.getType() != RedisDataType.STRING) {
                    throw new RedisCommandException(
                            "WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                oldValue = oldObj.getTypedData();
            }

            // 设置新值
            RedisObject newObj = RedisObject.createString(newValue);
            database.set(key, newObj);

            // 返回旧值
            if (oldValue == null) {
                return "$-1\r\n";
            }
            return "$" + oldValue.length() + "\r\n" + oldValue + "\r\n";
        } catch (RedisException e) {
            throw new RedisCommandException(e.getMessage());
        }
    }

    private String executeSet(RedisCommand command) {
        if (!command.checkArgCount(2)) {
            throw new RedisCommandException("wrong number of arguments for 'set' command");
        }

        String key = command.getArg(0);
        String value = command.getArg(1);

        logger.debug("SET(Command) 键: '{}', 值: '{}'", key, value);

        // 处理引号
        if (value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1);
        } else if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length() - 1);
        }

        if (key == null || key.isEmpty()) {
            throw new RedisCommandException("key cannot be null or empty");
        }
        if (value == null) {
            throw new RedisCommandException("value cannot be null");
        }

        try {
            RedisObject obj = RedisObject.createString(value);
            command.getDatabase().set(key, obj);
            return "OK"; // 修改为返回OK而不是+OK
        } catch (RedisException e) {
            throw new RedisCommandException(e.getMessage() != null ? e.getMessage() : "internal error");
        }
    }

    private String executeGet(RedisCommand command) {
        if (!command.checkArgCount(1)) {
            throw new RedisCommandException("wrong number of arguments for 'get' command");
        }

        String key = command.getArg(0);
        logger.debug("GET(Command) 键: '{}'", key);

        RedisObject obj = command.getDatabase().get(key);

        if (obj == null) {
            return "(nil)";
        }

        if (obj.getType() != RedisDataType.STRING) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        String value = obj.getTypedData();
        logger.debug("GET(Command) 键: '{}', 获取的值: '{}'", key, value);

        return value; // 直接返回值，不添加引号
    }

    private String executeGetSet(RedisCommand command) {
        if (!command.checkArgCount(2)) {
            throw new RedisCommandException("wrong number of arguments for 'getset' command");
        }

        String key = command.getArg(0);
        String newValue = command.getArg(1);

        RedisObject oldObj = command.getDatabase().get(key);
        String oldValue = null;

        if (oldObj != null) {
            if (oldObj.getType() != RedisDataType.STRING) {
                throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            oldValue = oldObj.getTypedData();
        }

        RedisObject newObj = RedisObject.createString(newValue);
        command.getDatabase().set(key, newObj);

        return oldValue;
    }

    private String executeIncr(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'incr' command");
        }

        String key = args[0];
        if (key == null || key.isEmpty()) {
            throw new RedisCommandException("key cannot be null or empty");
        }

        try {
            RedisObject obj = database.get(key);
            long value = 0;

            if (obj != null) {
                if (obj.getType() != RedisDataType.STRING) {
                    throw new RedisCommandException(
                            "WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                String strValue = obj.getTypedData();
                try {
                    value = Long.parseLong(strValue);
                } catch (NumberFormatException e) {
                    throw new RedisCommandException("value is not an integer or out of range");
                }
            }

            value++;
            RedisObject newObj = RedisObject.createString(String.valueOf(value));
            database.set(key, newObj);

            return ":" + value + "\r\n";
        } catch (RedisException e) {
            throw new RedisCommandException(e.getMessage());
        }
    }

    private String executeDecr(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'decr' command");
        }

        String key = args[0];
        if (key == null || key.isEmpty()) {
            throw new RedisCommandException("key cannot be null or empty");
        }

        try {
            RedisObject obj = database.get(key);
            long value = 0;

            if (obj != null) {
                if (obj.getType() != RedisDataType.STRING) {
                    throw new RedisCommandException(
                            "WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                String strValue = obj.getTypedData();
                try {
                    value = Long.parseLong(strValue);
                } catch (NumberFormatException e) {
                    throw new RedisCommandException("value is not an integer or out of range");
                }
            }

            value--;
            RedisObject newObj = RedisObject.createString(String.valueOf(value));
            database.set(key, newObj);

            return ":" + value + "\r\n";
        } catch (RedisException e) {
            throw new RedisCommandException(e.getMessage());
        }
    }

    private String executeIncrBy(String[] args) {
        if (args == null || args.length != 2) {
            throw new RedisCommandException("wrong number of arguments for 'incrby' command");
        }

        String key = args[0];
        long increment;
        try {
            increment = Long.parseLong(args[1]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not an integer or out of range");
        }

        if (key == null || key.isEmpty()) {
            throw new RedisCommandException("key cannot be null or empty");
        }

        try {
            RedisObject obj = database.get(key);
            long value = 0;

            if (obj != null) {
                if (obj.getType() != RedisDataType.STRING) {
                    throw new RedisCommandException(
                            "WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                String strValue = obj.getTypedData();
                try {
                    value = Long.parseLong(strValue);
                } catch (NumberFormatException e) {
                    throw new RedisCommandException("value is not an integer or out of range");
                }
            }

            value += increment;
            RedisObject newObj = RedisObject.createString(String.valueOf(value));
            database.set(key, newObj);

            return ":" + value + "\r\n";
        } catch (RedisException e) {
            throw new RedisCommandException(e.getMessage());
        }
    }

    private String executeDecrBy(String[] args) {
        if (args == null || args.length != 2) {
            throw new RedisCommandException("wrong number of arguments for 'decrby' command");
        }

        String key = args[0];
        long decrement;
        try {
            decrement = Long.parseLong(args[1]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not an integer or out of range");
        }

        if (key == null || key.isEmpty()) {
            throw new RedisCommandException("key cannot be null or empty");
        }

        try {
            RedisObject obj = database.get(key);
            long value = 0;

            if (obj != null) {
                if (obj.getType() != RedisDataType.STRING) {
                    throw new RedisCommandException(
                            "WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                String strValue = obj.getTypedData();
                try {
                    value = Long.parseLong(strValue);
                } catch (NumberFormatException e) {
                    throw new RedisCommandException("value is not an integer or out of range");
                }
            }

            value -= decrement;
            RedisObject newObj = RedisObject.createString(String.valueOf(value));
            database.set(key, newObj);

            return ":" + value + "\r\n";
        } catch (RedisException e) {
            throw new RedisCommandException(e.getMessage());
        }
    }
}