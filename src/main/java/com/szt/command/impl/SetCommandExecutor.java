package com.szt.command.impl;

import com.redis.command.CommandExecutor;
import com.redis.common.exception.RedisCommandException;
import com.redis.core.data.RedisDataType;
import com.redis.core.data.RedisObject;
import com.redis.core.db.RedisDatabase;

import java.util.*;

/**
 * 集合命令执行器
 */
public class SetCommandExecutor implements CommandExecutor {

    private static final Set<String> SUPPORTED_COMMANDS = new HashSet<>(Arrays.asList(
            "sadd", "srem", "smembers", "sismember", "scard", "spop", "srandmember",
            "sinter", "sunion", "sdiff"));

    /**
     * 当前数据库
     */
    private final RedisDatabase database;

    /**
     * 构造函数
     */
    public SetCommandExecutor(RedisDatabase database) {
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
            case "sadd":
                return executeSAdd(args);
            case "srem":
                return executeSRem(args);
            case "smembers":
                return executeSMembers(args);
            case "sismember":
                return executeSIsMember(args);
            case "scard":
                return executeSCard(args);
            case "spop":
                return executeSPop(args);
            case "srandmember":
                return executeSRandMember(args);
            case "sinter":
                return executeSInter(args);
            case "sunion":
                return executeSUnion(args);
            case "sdiff":
                return executeSDiff(args);
            default:
                throw new RedisCommandException("Unsupported command: " + cmdName);
        }
    }

    private String executeSAdd(String[] args) {
        if (args == null || args.length < 2) {
            throw new RedisCommandException("wrong number of arguments for 'sadd' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);
        Set<String> set;

        if (obj == null) {
            // 如果键不存在，创建新集合
            obj = RedisObject.createSet();
            database.set(key, obj);
        } else if (obj.getType() != RedisDataType.SET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        set = obj.getTypedData();

        // 添加元素到集合
        int added = 0;
        for (int i = 1; i < args.length; i++) {
            if (set.add(args[i])) {
                added++;
            }
        }

        return ":" + added + "\r\n";
    }

    private String executeSRem(String[] args) {
        if (args == null || args.length < 2) {
            throw new RedisCommandException("wrong number of arguments for 'srem' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return ":0\r\n"; // 键不存在
        }

        // 检查类型
        if (obj.getType() != RedisDataType.SET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Set<String> set = obj.getTypedData();
        int removed = 0;

        // 从集合中删除元素
        for (int i = 1; i < args.length; i++) {
            if (set.remove(args[i])) {
                removed++;
            }
        }

        return ":" + removed + "\r\n";
    }

    private String executeSMembers(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'smembers' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "*0\r\n"; // 键不存在
        }

        // 检查类型
        if (obj.getType() != RedisDataType.SET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Set<String> set = obj.getTypedData();
        List<String> members = new ArrayList<>(set);
        Collections.sort(members); // 按字典序排序

        StringBuilder result = new StringBuilder();
        result.append("*").append(members.size()).append("\r\n");
        for (String member : members) {
            result.append("$").append(member.length()).append("\r\n");
            result.append(member).append("\r\n");
        }
        return result.toString();
    }

    private String executeSIsMember(String[] args) {
        if (args == null || args.length != 2) {
            throw new RedisCommandException("wrong number of arguments for 'sismember' command");
        }

        String key = args[0];
        String member = args[1];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return ":0\r\n"; // 键不存在
        }

        // 检查类型
        if (obj.getType() != RedisDataType.SET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Set<String> set = obj.getTypedData();
        return ":" + (set.contains(member) ? "1" : "0") + "\r\n";
    }

    private String executeSCard(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'scard' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return ":0\r\n"; // 键不存在
        }

        // 检查类型
        if (obj.getType() != RedisDataType.SET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Set<String> set = obj.getTypedData();
        return ":" + set.size() + "\r\n";
    }

    private String executeSPop(String[] args) {
        if (args == null || args.length < 1 || args.length > 2) {
            throw new RedisCommandException("wrong number of arguments for 'spop' command");
        }

        String key = args[0];
        int count = 1;

        if (args.length == 2) {
            try {
                count = Integer.parseInt(args[1]);
                if (count < 0) {
                    throw new RedisCommandException("value is out of range, must be positive");
                }
            } catch (NumberFormatException e) {
                throw new RedisCommandException("value is not an integer or out of range");
            }
        }

        RedisObject obj = database.get(key);
        if (obj == null) {
            return count == 1 ? "$-1\r\n" : "*0\r\n";
        }

        if (obj.getType() != RedisDataType.SET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Set<String> set = obj.getTypedData();
        if (set.isEmpty()) {
            return count == 1 ? "$-1\r\n" : "*0\r\n";
        }

        if (count == 1) {
            String member = set.iterator().next();
            set.remove(member);
            return "$" + member.length() + "\r\n" + member + "\r\n";
        } else {
            List<String> result = new ArrayList<>();
            Iterator<String> it = set.iterator();
            while (it.hasNext() && result.size() < count) {
                String member = it.next();
                result.add(member);
                it.remove();
            }

            StringBuilder sb = new StringBuilder();
            sb.append("*").append(result.size()).append("\r\n");
            for (String member : result) {
                sb.append("$").append(member.length()).append("\r\n");
                sb.append(member).append("\r\n");
            }
            return sb.toString();
        }
    }

    private String executeSRandMember(String[] args) {
        if (args == null || args.length < 1 || args.length > 2) {
            throw new RedisCommandException("wrong number of arguments for 'srandmember' command");
        }

        String key = args[0];
        int count = 1;

        if (args.length == 2) {
            try {
                count = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                throw new RedisCommandException("value is not an integer or out of range");
            }
        }

        RedisObject obj = database.get(key);
        if (obj == null) {
            return count == 1 ? "$-1\r\n" : "*0\r\n";
        }

        if (obj.getType() != RedisDataType.SET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Set<String> set = obj.getTypedData();
        if (set.isEmpty()) {
            return count == 1 ? "$-1\r\n" : "*0\r\n";
        }

        if (count == 1) {
            String member = new ArrayList<>(set).get(new Random().nextInt(set.size()));
            return "$" + member.length() + "\r\n" + member + "\r\n";
        } else {
            List<String> members = new ArrayList<>(set);
            List<String> result = new ArrayList<>();
            Random random = new Random();

            if (count >= 0) {
                // 不允许重复
                int size = Math.min(count, members.size());
                while (result.size() < size) {
                    result.add(members.remove(random.nextInt(members.size())));
                }
            } else {
                // 允许重复
                count = -count;
                while (result.size() < count) {
                    result.add(members.get(random.nextInt(members.size())));
                }
            }

            StringBuilder sb = new StringBuilder();
            sb.append("*").append(result.size()).append("\r\n");
            for (String member : result) {
                sb.append("$").append(member.length()).append("\r\n");
                sb.append(member).append("\r\n");
            }
            return sb.toString();
        }
    }

    private String executeSInter(String[] args) {
        if (args == null || args.length < 1) {
            throw new RedisCommandException("wrong number of arguments for 'sinter' command");
        }

        Set<String> result = null;
        for (String key : args) {
            RedisObject obj = database.get(key);
            if (obj == null) {
                return "*0\r\n";
            }

            if (obj.getType() != RedisDataType.SET) {
                throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            Set<String> set = obj.getTypedData();
            if (result == null) {
                result = new HashSet<>(set);
            } else {
                result.retainAll(set);
            }

            if (result.isEmpty()) {
                return "*0\r\n";
            }
        }

        if (result == null) {
            return "*0\r\n";
        }

        List<String> sortedResult = new ArrayList<>(result);
        Collections.sort(sortedResult);

        StringBuilder sb = new StringBuilder();
        sb.append("*").append(sortedResult.size()).append("\r\n");
        for (String member : sortedResult) {
            sb.append("$").append(member.length()).append("\r\n");
            sb.append(member).append("\r\n");
        }
        return sb.toString();
    }

    private String executeSUnion(String[] args) {
        if (args == null || args.length < 1) {
            throw new RedisCommandException("wrong number of arguments for 'sunion' command");
        }

        Set<String> result = new HashSet<>();
        for (String key : args) {
            RedisObject obj = database.get(key);
            if (obj == null) {
                continue;
            }

            if (obj.getType() != RedisDataType.SET) {
                throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            Set<String> set = obj.getTypedData();
            result.addAll(set);
        }

        List<String> sortedResult = new ArrayList<>(result);
        Collections.sort(sortedResult);

        StringBuilder sb = new StringBuilder();
        sb.append("*").append(sortedResult.size()).append("\r\n");
        for (String member : sortedResult) {
            sb.append("$").append(member.length()).append("\r\n");
            sb.append(member).append("\r\n");
        }
        return sb.toString();
    }

    private String executeSDiff(String[] args) {
        if (args == null || args.length < 1) {
            throw new RedisCommandException("wrong number of arguments for 'sdiff' command");
        }

        RedisObject firstObj = database.get(args[0]);
        if (firstObj == null) {
            return "*0\r\n";
        }

        if (firstObj.getType() != RedisDataType.SET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Set<String> result = new HashSet<>(firstObj.<Set<String>>getTypedData());

        for (int i = 1; i < args.length; i++) {
            RedisObject obj = database.get(args[i]);
            if (obj == null) {
                continue;
            }

            if (obj.getType() != RedisDataType.SET) {
                throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            Set<String> set = obj.getTypedData();
            result.removeAll(set);

            if (result.isEmpty()) {
                return "*0\r\n";
            }
        }

        List<String> sortedResult = new ArrayList<>(result);
        Collections.sort(sortedResult);

        StringBuilder sb = new StringBuilder();
        sb.append("*").append(sortedResult.size()).append("\r\n");
        for (String member : sortedResult) {
            sb.append("$").append(member.length()).append("\r\n");
            sb.append(member).append("\r\n");
        }
        return sb.toString();
    }
}