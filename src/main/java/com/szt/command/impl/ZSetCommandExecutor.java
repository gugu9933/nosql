package com.szt.command.impl;

import com.redis.command.CommandExecutor;
import com.redis.common.exception.RedisCommandException;
import com.redis.core.data.RedisDataType;
import com.redis.core.data.RedisObject;
import com.redis.core.db.RedisDatabase;

import java.util.*;

/**
 * 有序集合命令执行器
 */
public class ZSetCommandExecutor implements CommandExecutor {

    private static final Set<String> SUPPORTED_COMMANDS = new HashSet<>(Arrays.asList(
            "zadd", "zcard", "zcount", "zincrby", "zrange", "zrank",
            "zrem", "zrevrange", "zrevrank", "zscore"));

    private final RedisDatabase database;

    public ZSetCommandExecutor(RedisDatabase database) {
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
            case "zadd":
                return executeZAdd(args);
            case "zcard":
                return executeZCard(args);
            case "zcount":
                return executeZCount(args);
            case "zincrby":
                return executeZIncrBy(args);
            case "zrange":
                return executeZRange(args);
            case "zrank":
                return executeZRank(args);
            case "zrem":
                return executeZRem(args);
            case "zrevrange":
                return executeZRevRange(args);
            case "zrevrank":
                return executeZRevRank(args);
            case "zscore":
                return executeZScore(args);
            default:
                throw new RedisCommandException("不支持的命令: " + cmdName);
        }
    }

    private String executeZAdd(String[] args) {
        if (args == null || args.length < 3 || args.length % 2 != 1) {
            throw new RedisCommandException("wrong number of arguments for 'zadd' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);
        TreeMap<Double, Set<String>> scoreMap;
        Map<String, Double> memberMap;

        if (obj == null) {
            // 如果键不存在，创建新有序集合
            obj = RedisObject.createZSet();
            database.set(key, obj);
            Object[] data = obj.getTypedData();
            scoreMap = (TreeMap<Double, Set<String>>) data[0];
            memberMap = (Map<String, Double>) data[1];
        } else if (obj.getType() != RedisDataType.ZSET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        } else {
            Object[] data = obj.getTypedData();
            scoreMap = (TreeMap<Double, Set<String>>) data[0];
            memberMap = (Map<String, Double>) data[1];
        }

        int added = 0;
        for (int i = 1; i < args.length; i += 2) {
            double score;
            try {
                score = Double.parseDouble(args[i]);
            } catch (NumberFormatException e) {
                throw new RedisCommandException("value is not a valid float");
            }

            String member = args[i + 1];
            Double oldScore = memberMap.get(member);

            if (oldScore != null) {
                // 移除旧分数
                Set<String> members = scoreMap.get(oldScore);
                if (members != null) {
                    members.remove(member);
                    if (members.isEmpty()) {
                        scoreMap.remove(oldScore);
                    }
                }
            } else {
                added++;
            }

            // 添加新分数
            scoreMap.computeIfAbsent(score, k -> new HashSet<>()).add(member);
            memberMap.put(member, score);
        }

        return String.valueOf(added);
    }

    private String executeZCard(String[] args) {
        if (args == null || args.length != 1) {
            throw new RedisCommandException("wrong number of arguments for 'zcard' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "0";
        }

        if (obj.getType() != RedisDataType.ZSET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Object[] data = obj.getTypedData();
        Map<String, Double> memberMap = (Map<String, Double>) data[1];
        return String.valueOf(memberMap.size());
    }

    private String executeZCount(String[] args) {
        if (args == null || args.length != 3) {
            throw new RedisCommandException("wrong number of arguments for 'zcount' command");
        }

        String key = args[0];
        double min, max;

        try {
            min = Double.parseDouble(args[1]);
            max = Double.parseDouble(args[2]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not a valid float");
        }

        RedisObject obj = database.get(key);
        if (obj == null) {
            return "0";
        }

        if (obj.getType() != RedisDataType.ZSET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Object[] data = obj.getTypedData();
        TreeMap<Double, Set<String>> scoreMap = (TreeMap<Double, Set<String>>) data[0];

        int count = 0;
        for (Map.Entry<Double, Set<String>> entry : scoreMap.subMap(min, true, max, true).entrySet()) {
            count += entry.getValue().size();
        }

        return String.valueOf(count);
    }

    private String executeZIncrBy(String[] args) {
        if (args == null || args.length != 3) {
            throw new RedisCommandException("wrong number of arguments for 'zincrby' command");
        }

        String key = args[0];
        double increment;
        String member = args[2];

        try {
            increment = Double.parseDouble(args[1]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not a valid float");
        }

        RedisObject obj = database.get(key);
        TreeMap<Double, Set<String>> scoreMap;
        Map<String, Double> memberMap;

        if (obj == null) {
            // 如果键不存在，创建新有序集合
            obj = RedisObject.createZSet();
            database.set(key, obj);
            Object[] data = obj.getTypedData();
            scoreMap = (TreeMap<Double, Set<String>>) data[0];
            memberMap = (Map<String, Double>) data[1];
        } else if (obj.getType() != RedisDataType.ZSET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        } else {
            Object[] data = obj.getTypedData();
            scoreMap = (TreeMap<Double, Set<String>>) data[0];
            memberMap = (Map<String, Double>) data[1];
        }

        Double oldScore = memberMap.get(member);
        double newScore = (oldScore != null ? oldScore : 0) + increment;

        if (oldScore != null) {
            // 移除旧分数
            Set<String> members = scoreMap.get(oldScore);
            if (members != null) {
                members.remove(member);
                if (members.isEmpty()) {
                    scoreMap.remove(oldScore);
                }
            }
        }

        // 添加新分数
        scoreMap.computeIfAbsent(newScore, k -> new HashSet<>()).add(member);
        memberMap.put(member, newScore);

        return String.valueOf(newScore);
    }

    private String executeZRange(String[] args) {
        if (args == null || args.length < 3) {
            throw new RedisCommandException("wrong number of arguments for 'zrange' command");
        }

        String key = args[0];
        int start, stop;
        boolean withScores = args.length > 3 && "withscores".equalsIgnoreCase(args[3]);

        try {
            start = Integer.parseInt(args[1]);
            stop = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not an integer or out of range");
        }

        RedisObject obj = database.get(key);
        if (obj == null) {
            return "[]";
        }

        if (obj.getType() != RedisDataType.ZSET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Object[] data = obj.getTypedData();
        TreeMap<Double, Set<String>> scoreMap = (TreeMap<Double, Set<String>>) data[0];
        Map<String, Double> memberMap = (Map<String, Double>) data[1];

        List<String> result = new ArrayList<>();
        List<String> members = new ArrayList<>();

        // 收集所有成员（按分数排序）
        for (Map.Entry<Double, Set<String>> entry : scoreMap.entrySet()) {
            members.addAll(new TreeSet<>(entry.getValue())); // 使用TreeSet确保成员按字典序排序
        }

        // 处理负索引
        int size = members.size();
        if (start < 0)
            start = size + start;
        if (stop < 0)
            stop = size + stop;

        // 限制范围
        start = Math.max(0, start);
        stop = Math.min(size - 1, stop);

        if (start > stop || start >= size) {
            return "[]";
        }

        // 提取指定范围的成员
        for (int i = start; i <= stop && i < size; i++) {
            String member = members.get(i);
            result.add(member);
            if (withScores) {
                result.add(String.valueOf(memberMap.get(member)));
            }
        }

        return result.toString();
    }

    private String executeZRank(String[] args) {
        if (args == null || args.length != 2) {
            throw new RedisCommandException("wrong number of arguments for 'zrank' command");
        }

        String key = args[0];
        String member = args[1];

        RedisObject obj = database.get(key);
        if (obj == null) {
            return null;
        }

        if (obj.getType() != RedisDataType.ZSET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Object[] data = obj.getTypedData();
        TreeMap<Double, Set<String>> scoreMap = (TreeMap<Double, Set<String>>) data[0];
        Map<String, Double> memberMap = (Map<String, Double>) data[1];

        if (!memberMap.containsKey(member)) {
            return null;
        }

        int rank = 0;
        double targetScore = memberMap.get(member);

        for (Map.Entry<Double, Set<String>> entry : scoreMap.entrySet()) {
            if (entry.getKey() < targetScore) {
                rank += entry.getValue().size();
            } else if (entry.getKey().equals(targetScore)) {
                for (String m : entry.getValue()) {
                    if (m.equals(member)) {
                        break;
                    }
                    rank++;
                }
                break;
            }
        }

        return String.valueOf(rank);
    }

    private String executeZRem(String[] args) {
        if (args == null || args.length < 2) {
            throw new RedisCommandException("wrong number of arguments for 'zrem' command");
        }

        String key = args[0];
        RedisObject obj = database.get(key);

        if (obj == null) {
            return "0";
        }

        if (obj.getType() != RedisDataType.ZSET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Object[] data = obj.getTypedData();
        TreeMap<Double, Set<String>> scoreMap = (TreeMap<Double, Set<String>>) data[0];
        Map<String, Double> memberMap = (Map<String, Double>) data[1];

        int removed = 0;
        for (int i = 1; i < args.length; i++) {
            String member = args[i];
            Double score = memberMap.remove(member);

            if (score != null) {
                Set<String> members = scoreMap.get(score);
                if (members != null) {
                    members.remove(member);
                    if (members.isEmpty()) {
                        scoreMap.remove(score);
                    }
                }
                removed++;
            }
        }

        return String.valueOf(removed);
    }

    private String executeZRevRange(String[] args) {
        if (args == null || args.length < 3) {
            throw new RedisCommandException("wrong number of arguments for 'zrevrange' command");
        }

        String key = args[0];
        int start, stop;
        boolean withScores = args.length > 3 && "withscores".equalsIgnoreCase(args[3]);

        try {
            start = Integer.parseInt(args[1]);
            stop = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            throw new RedisCommandException("value is not an integer or out of range");
        }

        RedisObject obj = database.get(key);
        if (obj == null) {
            return "[]";
        }

        if (obj.getType() != RedisDataType.ZSET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Object[] data = obj.getTypedData();
        TreeMap<Double, Set<String>> scoreMap = (TreeMap<Double, Set<String>>) data[0];

        List<String> result = new ArrayList<>();
        List<String> members = new ArrayList<>();

        // 收集所有成员（倒序）
        for (Set<String> memberSet : scoreMap.descendingMap().values()) {
            members.addAll(memberSet);
        }

        // 处理负索引
        int size = members.size();
        if (start < 0)
            start = size + start;
        if (stop < 0)
            stop = size + stop;

        // 限制范围
        start = Math.max(0, start);
        stop = Math.min(size - 1, stop);

        if (start > stop || start >= size) {
            return "[]";
        }

        // 提取指定范围的成员
        for (int i = start; i <= stop && i < size; i++) {
            String member = members.get(i);
            result.add(member);
            if (withScores) {
                result.add(String.valueOf(((Map<String, Double>) data[1]).get(member)));
            }
        }

        return result.toString();
    }

    private String executeZRevRank(String[] args) {
        if (args == null || args.length != 2) {
            throw new RedisCommandException("wrong number of arguments for 'zrevrank' command");
        }

        String key = args[0];
        String member = args[1];

        RedisObject obj = database.get(key);
        if (obj == null) {
            return null;
        }

        if (obj.getType() != RedisDataType.ZSET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Object[] data = obj.getTypedData();
        TreeMap<Double, Set<String>> scoreMap = (TreeMap<Double, Set<String>>) data[0];
        Map<String, Double> memberMap = (Map<String, Double>) data[1];

        if (!memberMap.containsKey(member)) {
            return null;
        }

        int rank = 0;
        double targetScore = memberMap.get(member);

        for (Map.Entry<Double, Set<String>> entry : scoreMap.descendingMap().entrySet()) {
            if (entry.getKey() > targetScore) {
                rank += entry.getValue().size();
            } else if (entry.getKey().equals(targetScore)) {
                for (String m : entry.getValue()) {
                    if (m.equals(member)) {
                        break;
                    }
                    rank++;
                }
                break;
            }
        }

        return String.valueOf(rank);
    }

    private String executeZScore(String[] args) {
        if (args == null || args.length != 2) {
            throw new RedisCommandException("wrong number of arguments for 'zscore' command");
        }

        String key = args[0];
        String member = args[1];

        RedisObject obj = database.get(key);
        if (obj == null) {
            return null;
        }

        if (obj.getType() != RedisDataType.ZSET) {
            throw new RedisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        Object[] data = obj.getTypedData();
        Map<String, Double> memberMap = (Map<String, Double>) data[1];
        Double score = memberMap.get(member);

        return score != null ? String.valueOf(score) : null;
    }
}