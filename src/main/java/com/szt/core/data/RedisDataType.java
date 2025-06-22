package com.szt.core.data;

import java.util.*;

/**
 * Redis数据类型枚举
 */
public enum RedisDataType {
    STRING("string", String.class),
    LIST("list", List.class),
    SET("set", Set.class),
    ZSET("zset", Object[].class),
    HASH("hash", Map.class);

    private final String type;
    private final Class<?> dataClass;

    RedisDataType(String type, Class<?> dataClass) {
        this.type = type;
        this.dataClass = dataClass;
    }

    public String getType() {
        return type;
    }

    public Class<?> getDataClass() {
        return dataClass;
    }

    /**
     * 检查数据类型是否匹配
     */
    public boolean checkType(Object data) {
        if (data == null) {
            return false;
        }

        switch (this) {
            case STRING:
                return data instanceof String;
            case LIST:
                return data instanceof List && checkListContent((List<?>) data);
            case SET:
                return data instanceof Set && checkSetContent((Set<?>) data);
            case ZSET:
                return data instanceof Object[] && checkZSetContent((Object[]) data);
            case HASH:
                return data instanceof Map && checkMapContent((Map<?, ?>) data);
            default:
                return false;
        }
    }

    /**
     * 检查列表内容是否合法
     */
    private boolean checkListContent(List<?> list) {
        return list.stream().allMatch(item -> item == null || item instanceof String);
    }

    /**
     * 检查集合内容是否合法
     */
    private boolean checkSetContent(Set<?> set) {
        return set.stream().allMatch(item -> item == null || item instanceof String);
    }

    /**
     * 检查有序集合内容是否合法
     */
    private boolean checkZSetContent(Object[] zsetData) {
        if (zsetData.length != 2) {
            return false;
        }

        if (!(zsetData[0] instanceof TreeMap) || !(zsetData[1] instanceof Map)) {
            return false;
        }

        TreeMap<?, ?> scoreMap = (TreeMap<?, ?>) zsetData[0];
        Map<?, ?> memberMap = (Map<?, ?>) zsetData[1];

        // 检查score -> members映射
        boolean validScoreMap = scoreMap.keySet().stream().allMatch(key -> key instanceof Double) &&
                scoreMap.values().stream().allMatch(value -> value instanceof Set &&
                        ((Set<?>) value).stream().allMatch(member -> member instanceof String));

        // 检查member -> score映射
        boolean validMemberMap = memberMap.keySet().stream().allMatch(key -> key instanceof String) &&
                memberMap.values().stream().allMatch(value -> value instanceof Double);

        return validScoreMap && validMemberMap;
    }

    /**
     * 检查哈希表内容是否合法
     */
    private boolean checkMapContent(Map<?, ?> map) {
        return map.keySet().stream().allMatch(key -> key instanceof String) &&
                map.values().stream().allMatch(value -> value == null || value instanceof String);
    }

    /**
     * 从字符串创建数据类型
     */
    public static RedisDataType fromString(String type) {
        if (type == null) {
            throw new IllegalArgumentException("Type cannot be null");
        }
        for (RedisDataType dataType : values()) {
            if (dataType.type.equalsIgnoreCase(type)) {
                return dataType;
            }
        }
        throw new IllegalArgumentException("Unknown data type: " + type);
    }

    /**
     * 获取类型的友好名称
     */
    public String getFriendlyName() {
        return type;
    }

    /**
     * 创建对应类型的空容器
     */
    public Object createEmptyContainer() {
        switch (this) {
            case STRING:
                return "";
            case LIST:
                return new ArrayList<String>();
            case SET:
                return new HashSet<String>();
            case ZSET:
                Object[] zsetData = new Object[2];
                zsetData[0] = new TreeMap<Double, Set<String>>();
                zsetData[1] = new HashMap<String, Double>();
                return zsetData;
            case HASH:
                return new HashMap<String, String>();
            default:
                throw new IllegalStateException("Unknown data type: " + this);
        }
    }
}