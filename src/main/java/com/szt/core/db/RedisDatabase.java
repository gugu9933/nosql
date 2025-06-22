package com.szt.core.db;

import com.szt.common.exception.RedisException;
import com.szt.core.data.RedisObject;
import com.szt.event.KeyEvent;
import com.szt.event.KeyEventPublisher;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Redis数据库
 * 负责存储和管理键值对
 */
public class RedisDatabase implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(RedisDatabase.class);
    private static final long serialVersionUID = 1L;

    /**
     * 数据库索引
     */
    @Getter
    private final int index;

    /**
     * 键值存储
     */
    private final Map<String, RedisObject> data;

    /**
     * 事件发布器
     */
    private transient KeyEventPublisher eventPublisher;

    /**
     * 过期键清理调度器
     */
    private transient ScheduledExecutorService expirationScheduler;

    /**
     * 构造函数
     *
     * @param index          数据库索引
     * @param eventPublisher 事件发布器
     */
    public RedisDatabase(int index, KeyEventPublisher eventPublisher) {
        this.index = index;
        this.eventPublisher = eventPublisher;
        this.data = new ConcurrentHashMap<>();
        initExpirationScheduler();
    }

    /**
     * 初始化过期键清理调度器
     */
    private void initExpirationScheduler() {
        expirationScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "expiration-scheduler-" + index);
            t.setDaemon(true);
            return t;
        });

        // 每秒检查一次过期键
        expirationScheduler.scheduleAtFixedRate(this::cleanupExpiredKeys, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * 清理过期键
     */
    private void cleanupExpiredKeys() {
        try {
            int cleaned = 0;
            for (Iterator<Map.Entry<String, RedisObject>> it = data.entrySet().iterator(); it.hasNext();) {
                Map.Entry<String, RedisObject> entry = it.next();
                RedisObject obj = entry.getValue();
                if (obj.isExpired()) {
                    String key = entry.getKey();
                    it.remove();
                    if (eventPublisher != null) {
                        eventPublisher.publishEvent(KeyEvent.createExpiredEvent(this, key, obj));
                    }
                    cleaned++;
                }
            }
            if (cleaned > 0) {
                logger.debug("Cleaned {} expired keys from database {}", cleaned, index);
            }
        } catch (Exception e) {
            logger.error("Error while cleaning expired keys", e);
        }
    }

    /**
     * 设置事件发布器（用于反序列化后恢复）
     *
     * @param eventPublisher 事件发布器
     */
    public void setEventPublisher(KeyEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
        if (expirationScheduler == null || expirationScheduler.isShutdown()) {
            initExpirationScheduler();
        }
    }

    /**
     * 获取键的数量
     *
     * @return 键的数量
     */
    public int size() {
        return data.size();
    }

    /**
     * 获取所有键
     *
     * @return 键集合
     */
    public Set<String> getKeys() {
        return data.keySet();
    }

    /**
     * 设置键值
     *
     * @param key   键
     * @param value 值
     * @throws RedisException 如果键或值为null
     */
    public void set(String key, RedisObject value) {
        if (key == null) {
            throw new RedisException("key cannot be null");
        }
        if (value == null) {
            throw new RedisException("value cannot be null");
        }

        RedisObject oldValue = data.get(key);
        data.put(key, value);

        // 发布事件
        if (oldValue == null) {
            eventPublisher.publishEvent(KeyEvent.createAddEvent(this, key, value));
        } else {
            eventPublisher.publishEvent(KeyEvent.createUpdateEvent(this, key, value, oldValue));
        }
    }

    /**
     * 获取值
     *
     * @param key 键
     * @return 值对象
     */
    public RedisObject get(String key) {
        if (key == null) {
            throw new RedisException("key cannot be null");
        }

        RedisObject value = data.get(key);
        if (value != null && value.isExpired()) {
            delete(key);
            return null;
        }
        return value;
    }

    /**
     * 删除键
     *
     * @param key 键
     * @return 被删除的值
     */
    public RedisObject delete(String key) {
        if (key == null) {
            throw new RedisException("key cannot be null");
        }

        RedisObject value = data.remove(key);
        if (value != null) {
            eventPublisher.publishEvent(KeyEvent.createDeleteEvent(this, key, value));
        }
        return value;
    }

    /**
     * 检查键是否存在
     *
     * @param key 键
     * @return 是否存在
     */
    public boolean exists(String key) {
        return data.containsKey(key);
    }

    /**
     * 获取键的剩余生存时间（毫秒）
     */
    public long ttl(String key) {
        RedisObject value = get(key);
        if (value == null) {
            return -2; // 键不存在
        }
        if (value.getExpireTime() == null) {
            return -1; // 键永不过期
        }
        return Math.max(0, value.getExpireTime() - System.currentTimeMillis());
    }

    /**
     * 设置过期时间
     *
     * @param key          键
     * @param milliseconds 过期时间（毫秒）
     * @return 是否设置成功
     */
    public boolean expire(String key, long milliseconds) {
        RedisObject value = data.get(key);
        if (value == null) {
            return false;
        }

        // 计算过期时间点
        long expireAt = System.currentTimeMillis() + milliseconds;
        value.setExpireTime(expireAt);

        // 发布过期事件
        if (eventPublisher != null) {
            KeyEvent event = KeyEvent.createExpireEvent(this, key, value, milliseconds);
            eventPublisher.publishEvent(event);
        }

        return true;
    }

    /**
     * 清空数据库
     */
    public void clear() {
        // 保存所有键的副本，用于发布事件
        Map<String, RedisObject> oldData = new HashMap<>(data);

        // 清空数据
        data.clear();

        // 发布清空事件
        if (eventPublisher != null) {
            for (Map.Entry<String, RedisObject> entry : oldData.entrySet()) {
                KeyEvent event = KeyEvent.createDeleteEvent(this, entry.getKey(), entry.getValue());
                eventPublisher.publishEvent(event);
            }
        }
    }

    /**
     * 设置值并设置过期时间
     *
     * @param key          键
     * @param value        值
     * @param milliseconds 过期时间（毫秒）
     */
    public void setWithExpire(String key, String value, long milliseconds) {
        RedisObject valueObj = RedisObject.createString(value);
        set(key, valueObj);
        expire(key, milliseconds);
    }

    public void setExpire(String key, long expireTime) {
        RedisObject value = get(key);
        if (value != null) {
            value.setExpireAt(expireTime);
            eventPublisher.publishEvent(KeyEvent.createExpireEvent(this, key, value, expireTime));
        }
    }

    /**
     * 获取所有键值对数据
     *
     * @return 键值对数据
     */
    public Map<String, RedisObject> getAllData() {
        return new HashMap<>(data);
    }

    /**
     * 检查键是否已过期
     *
     * @param key 键
     * @return 是否已过期
     */
    public boolean isExpired(String key) {
        if (key == null) {
            return false;
        }

        RedisObject obj = data.get(key);
        return obj != null && obj.isExpired();
    }

    /**
     * 关闭数据库
     */
    public void shutdown() {
        if (expirationScheduler != null) {
            expirationScheduler.shutdown();
            try {
                if (!expirationScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    expirationScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                expirationScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 获取事件发布器
     *
     * @return 事件发布器
     */
    public KeyEventPublisher getEventPublisher() {
        return eventPublisher;
    }
}