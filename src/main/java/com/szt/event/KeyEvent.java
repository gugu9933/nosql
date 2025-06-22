package com.szt.event;

import com.szt.core.data.RedisObject;
import lombok.Getter;

import java.util.EventObject;

/**
 * 键事件
 * 表示对键的操作事件
 */
@Getter
public class KeyEvent extends EventObject {
    private static final long serialVersionUID = 1L;

    /**
     * 事件类型
     */
    private final Type type;

    /**
     * 键名
     */
    private final String key;

    /**
     * 当前值
     */
    private final RedisObject value;

    /**
     * 旧值（仅在更新事件中有效）
     */
    private final RedisObject oldValue;

    /**
     * 过期时间（毫秒，仅在过期事件中有效）
     */
    private final Long expireTime;

    /**
     * 私有构造函数
     *
     * @param source     事件源
     * @param type       事件类型
     * @param key        键名
     * @param value      当前值
     * @param oldValue   旧值
     * @param expireTime 过期时间
     */
    private KeyEvent(Object source, Type type, String key, RedisObject value, RedisObject oldValue, Long expireTime) {
        super(source);
        this.type = type;
        this.key = key;
        this.value = value;
        this.oldValue = oldValue;
        this.expireTime = expireTime;
    }

    /**
     * 创建添加事件
     *
     * @param source 事件源
     * @param key    键名
     * @param value  值
     * @return 键事件
     */
    public static KeyEvent createAddEvent(Object source, String key, RedisObject value) {
        if (source == null) {
            source = KeyEvent.class; // 使用KeyEvent类作为默认事件源
        }
        return new KeyEvent(source, Type.ADD, key, value, null, null);
    }

    /**
     * 创建更新事件
     *
     * @param source   事件源
     * @param key      键名
     * @param value    新值
     * @param oldValue 旧值
     * @return 键事件
     */
    public static KeyEvent createUpdateEvent(Object source, String key, RedisObject value, RedisObject oldValue) {
        if (source == null) {
            source = KeyEvent.class;
        }
        return new KeyEvent(source, Type.UPDATE, key, value, oldValue, null);
    }

    /**
     * 创建删除事件
     *
     * @param source 事件源
     * @param key    键名
     * @param value  被删除的值
     * @return 键事件
     */
    public static KeyEvent createDeleteEvent(Object source, String key, RedisObject value) {
        if (source == null) {
            source = KeyEvent.class;
        }
        return new KeyEvent(source, Type.DELETE, key, value, null, null);
    }

    /**
     * 创建过期事件
     *
     * @param source     事件源
     * @param key        键名
     * @param value      值
     * @param expireTime 过期时间（毫秒）
     * @return 键事件
     */
    public static KeyEvent createExpireEvent(Object source, String key, RedisObject value, long expireTime) {
        if (source == null) {
            source = KeyEvent.class;
        }
        return new KeyEvent(source, Type.EXPIRE, key, value, null, expireTime);
    }

    /**
     * 创建键过期事件
     *
     * @param source 事件源
     * @param key    键名
     * @param value  过期的值
     * @return 键事件
     */
    public static KeyEvent createExpiredEvent(Object source, String key, RedisObject value) {
        if (source == null) {
            source = KeyEvent.class;
        }
        return new KeyEvent(source, Type.EXPIRED, key, value, null, null);
    }

    /**
     * 事件类型枚举
     */
    public enum Type {
        /**
         * 添加键
         */
        ADD,

        /**
         * 更新键
         */
        UPDATE,

        /**
         * 删除键
         */
        DELETE,

        /**
         * 设置过期时间
         */
        EXPIRE,

        /**
         * 键过期
         */
        EXPIRED
    }
}