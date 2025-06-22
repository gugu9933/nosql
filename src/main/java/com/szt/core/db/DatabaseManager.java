package com.szt.core.db;

import com.szt.common.constant.RedisConstants;
import com.szt.common.exception.RedisException;
import com.szt.config.RedisConfig;
import com.szt.event.KeyEventPublisher;
import com.szt.storage.PersistenceManager;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 数据库管理器
 * 负责管理所有数据库实例和持久化操作
 */
public class DatabaseManager {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);

    /**
     * 数据库列表
     */
    @Getter
    private final List<RedisDatabase> databases;

    /**
     * 配置信息
     */
    private final RedisConfig config;

    /**
     * 事件发布器
     */
    private final KeyEventPublisher eventPublisher;

    /**
     * 持久化管理器
     */
    private PersistenceManager persistenceManager;

    /**
     * 定时任务执行器
     */
    private ScheduledExecutorService scheduler;

    /**
     * 构造函数
     *
     * @param config         配置信息
     * @param eventPublisher 事件发布器
     */
    public DatabaseManager(RedisConfig config, KeyEventPublisher eventPublisher) {
        this.config = config;
        this.eventPublisher = eventPublisher;
        this.databases = new ArrayList<>(config.getDatabaseCount());
    }

    /**
     * 初始化数据库
     */
    public void initialize() {
        // 创建数据库实例
        for (int i = 0; i < config.getDatabaseCount(); i++) {
            databases.add(new RedisDatabase(i, eventPublisher));
        }

        // 初始化持久化管理器
        initPersistenceManager();

        // 启动定时任务
        startScheduledTasks();
    }

    /**
     * 初始化持久化管理器
     */
    private void initPersistenceManager() {
        persistenceManager = new PersistenceManager(config);

        try {
            // 从持久化文件加载数据
            logger.info("Loading data from persistence storage...");
            persistenceManager.loadData(databases);

            // 启动定时持久化任务
            persistenceManager.startScheduledPersistence(databases);

            logger.info("Persistence manager initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize persistence manager", e);
            throw new RedisException("Failed to initialize persistence manager", e);
        }
    }

    /**
     * 启动定时任务
     */
    private void startScheduledTasks() {
        scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "redis-db-tasks");
            t.setDaemon(true);
            return t;
        });

        // 定时执行过期键清理
        scheduler.scheduleWithFixedDelay(
                this::cleanExpiredKeys,
                1,
                1,
                TimeUnit.SECONDS);

        // 如果是从节点，定期重新加载持久化文件（无论是否处于集群模式）
        if (!config.isMaster()) {
            int reloadInterval = 5; // 每5秒重新加载一次
            logger.info("从节点将每 {} 秒重新加载一次持久化文件 ({})，节点ID: {}, 集群模式: {}",
                    reloadInterval, config.getPersistenceMode(),
                    config.getNodeId(), config.isClusterEnabled() ? "是" : "否");

            scheduler.scheduleWithFixedDelay(
                    this::reloadPersistenceData,
                    reloadInterval,
                    reloadInterval,
                    TimeUnit.SECONDS);
        }
    }

    /**
     * 清理过期键
     */
    private void cleanExpiredKeys() {
        try {
            int totalExpired = 0;
            for (RedisDatabase db : databases) {
                if (db == null) {
                    continue;
                }

                Set<String> keys = db.getKeys();
                for (String key : keys) {
                    if (db.isExpired(key)) {
                        db.delete(key);
                        totalExpired++;
                    }
                }
            }

            if (totalExpired > 0) {
                logger.debug("Cleaned {} expired keys", totalExpired);
            }
        } catch (Exception e) {
            logger.error("Error cleaning expired keys", e);
        }
    }

    /**
     * 重新加载持久化数据
     */
    private void reloadPersistenceData() {
        try {
            logger.info("从节点开始重新加载持久化数据...");

            // 记录加载前的键数量
            int beforeKeysCount = countTotalKeys();

            // 加载持久化数据
            persistenceManager.loadData(databases);

            // 记录加载后的键数量
            int afterKeysCount = countTotalKeys();

            if (afterKeysCount > beforeKeysCount) {
                logger.info("持久化数据重新加载完成，键数量从 {} 增加到 {}", beforeKeysCount, afterKeysCount);
            } else if (afterKeysCount < beforeKeysCount) {
                logger.info("持久化数据重新加载完成，键数量从 {} 减少到 {}", beforeKeysCount, afterKeysCount);
            } else {
                logger.debug("持久化数据重新加载完成，键数量未变化 ({})", afterKeysCount);
            }
        } catch (Exception e) {
            logger.error("重新加载持久化数据失败", e);
        }
    }

    /**
     * 计算所有数据库中的键总数
     */
    private int countTotalKeys() {
        int totalKeys = 0;
        for (RedisDatabase db : databases) {
            if (db != null) {
                totalKeys += db.size();
            }
        }
        return totalKeys;
    }

    /**
     * 保存数据
     */
    private void saveData() {
        if (persistenceManager != null) {
            try {
                logger.info("Saving data to persistence storage");
                persistenceManager.saveData(databases);
                logger.info("Data saved successfully");
            } catch (Exception e) {
                logger.error("Failed to save data", e);
            }
        }
    }

    /**
     * 获取指定索引的数据库
     *
     * @param index 数据库索引
     * @return 数据库实例
     */
    public RedisDatabase getDatabase(int index) {
        if (index < 0 || index >= databases.size()) {
            throw new RedisException("Invalid database index: " + index);
        }
        return databases.get(index);
    }

    /**
     * 关闭数据库管理器
     */
    public void shutdown() {
        logger.info("Shutting down database manager");

        // 停止定时任务
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 关闭持久化管理器
        if (persistenceManager != null) {
            try {
                persistenceManager.shutdown(databases);
                logger.info("Persistence manager shut down successfully");
            } catch (Exception e) {
                logger.error("Error shutting down persistence manager", e);
            }
        }
    }

    /**
     * 记录命令到AOF文件
     * 
     * @param command 命令名称
     * @param args    命令参数
     */
    public void appendCommandToAof(String command, String[] args) {
        if (persistenceManager != null &&
                RedisConstants.PERSISTENCE_AOF.equals(config.getPersistenceMode())) {
            persistenceManager.appendCommand(command, args);
        }
    }
}