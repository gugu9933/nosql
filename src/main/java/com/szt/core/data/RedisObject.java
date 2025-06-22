package com.szt.core.data;

import com.szt.common.exception.RedisException;
import lombok.Data;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ScheduledFuture;

/**
 * Redis数据对象
 * 封装Redis中存储的数据
 */
@Data
public class RedisObject implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 数据类型
     */
    private RedisDataType type;

    /**
     * 实际数据
     */
    private Object data;

    /**
     * 创建时间
     */
    private long createTime;

    /**
     * 最后访问时间
     */
    private long lastAccessTime;

    /**
     * 过期时间（毫秒时间戳）
     */
    private Long expireTime;

    /**
     * 过期任务的Future
     */
    private transient ScheduledFuture<?> expiringFuture;

    /**
     * 私有构造函数
     */
    private RedisObject(RedisDataType type, Object data) {
        if (type == null) {
            throw new RedisException("数据类型不能为空");
        }
        if (data == null) {
            data = type.createEmptyContainer();
        }
        if (!type.checkType(data)) {
            throw new RedisException(String.format("数据类型不匹配: 期望 %s, 实际 %s",
                    type.getFriendlyName(), data.getClass().getSimpleName()));
        }
        this.type = type;
        this.data = data;
        this.createTime = System.currentTimeMillis();
        this.lastAccessTime = this.createTime;
        this.expireTime = null;
    }

    /**
     * 创建字符串对象
     */
    public static RedisObject createString(String value) {
        return new RedisObject(RedisDataType.STRING, value);
    }

    /**
     * 创建列表对象
     */
    public static RedisObject createList() {
        return new RedisObject(RedisDataType.LIST, new ArrayList<String>());
    }

    /**
     * 创建集合对象
     */
    public static RedisObject createSet() {
        return new RedisObject(RedisDataType.SET, new HashSet<String>());
    }

    /**
     * 创建有序集合对象
     */
    public static RedisObject createZSet() {
        Object[] zsetData = new Object[2];
        zsetData[0] = new TreeMap<Double, Set<String>>(); // score -> members mapping
        zsetData[1] = new HashMap<String, Double>(); // member -> score mapping
        return new RedisObject(RedisDataType.ZSET, zsetData);
    }

    /**
     * 创建哈希对象
     */
    public static RedisObject createHash() {
        return new RedisObject(RedisDataType.HASH, new HashMap<String, String>());
    }

    /**
     * 获取类型安全的数据
     */
    @SuppressWarnings("unchecked")
    public <T> T getTypedData() {
        updateAccessTime();
        if (data == null) {
            return null;
        }
        if (!type.checkType(data)) {
            throw new RedisException(String.format("数据类型不匹配: 期望 %s, 实际 %s",
                    type.getFriendlyName(), data.getClass().getSimpleName()));
        }
        return (T) data;
    }

    /**
     * 更新最后访问时间
     */
    public void updateAccessTime() {
        this.lastAccessTime = System.currentTimeMillis();
    }

    /**
     * 设置过期时间
     */
    public void setExpireTime(Long expireTime) {
        this.expireTime = expireTime;
        if (expiringFuture != null && !expiringFuture.isDone()) {
            expiringFuture.cancel(false);
        }
    }

    /**
     * 检查是否已过期
     */
    public boolean isExpired() {
        return expireTime != null && System.currentTimeMillis() > expireTime;
    }

    /**
     * 设置过期时间点
     */
    public void setExpireAt(long expireAt) {
        this.expireTime = expireAt;
        if (expiringFuture != null && !expiringFuture.isDone()) {
            expiringFuture.cancel(false);
        }
    }

    /**
     * 移除过期时间
     */
    public void persist() {
        this.expireTime = null;
        if (expiringFuture != null && !expiringFuture.isDone()) {
            expiringFuture.cancel(false);
        }
    }

    /**
     * 设置过期任务
     */
    public void setExpiringFuture(ScheduledFuture<?> future) {
        if (this.expiringFuture != null && !this.expiringFuture.isDone()) {
            this.expiringFuture.cancel(false);
        }
        this.expiringFuture = future;
    }
}