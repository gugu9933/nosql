package com.szt.storage;

import com.szt.common.constant.RedisConstants;
import com.szt.common.exception.RedisException;
import com.szt.config.RedisConfig;
import com.szt.core.db.RedisDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 持久化管理器
 * 负责数据的持久化和恢复
 */
public class PersistenceManager {
    private static final Logger logger = LoggerFactory.getLogger(PersistenceManager.class);

    /**
     * RDB持久化实现
     */
    private final RdbPersistence rdbPersistence;

    /**
     * AOF持久化实现
     */
    private final AofPersistence aofPersistence;

    /**
     * 配置信息
     */
    private final RedisConfig config;

    /**
     * 数据目录
     */
    private final File dataDir;

    /**
     * RDB文件
     */
    private final File rdbFile;

    /**
     * AOF文件
     */
    private final File aofFile;

    /**
     * 定时任务执行器
     */
    private final ScheduledExecutorService scheduler;

    /**
     * 构造函数
     *
     * @param config 配置信息
     */
    public PersistenceManager(RedisConfig config) {
        this.config = config;

        // 创建数据目录
        this.dataDir = new File("db");
        if (!dataDir.exists() && !dataDir.mkdirs()) {
            throw new RedisException("Failed to create data directory: " + dataDir.getAbsolutePath());
        }

        // 初始化文件
        this.rdbFile = new File(dataDir, "dump.rdb");
        this.aofFile = new File(dataDir, "appendonly.aof");

        // 初始化持久化实现
        this.rdbPersistence = new RdbPersistence(config);
        this.aofPersistence = new AofPersistence(config);

        // 初始化定时任务执行器
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "redis-persistence");
            t.setDaemon(true);
            return t;
        });

        // 初始化文件
        initFiles();
    }

    /**
     * 初始化文件
     */
    private void initFiles() {
        try {
            // 确保数据目录存在
            if (!dataDir.exists()) {
                logger.info("Creating data directory: {}", dataDir.getAbsolutePath());
                if (!dataDir.mkdirs()) {
                    throw new RedisException("Failed to create data directory: " + dataDir.getAbsolutePath());
                }
            }

            // 检查目录权限
            if (!dataDir.canWrite() || !dataDir.canRead()) {
                throw new RedisException("Insufficient permissions for data directory: " + dataDir.getAbsolutePath());
            }

            // 创建RDB文件
            if (!rdbFile.exists()) {
                logger.info("Creating RDB file: {}", rdbFile.getAbsolutePath());
                if (!rdbFile.createNewFile()) {
                    logger.error("Failed to create RDB file: {}", rdbFile.getAbsolutePath());
                    throw new RedisException("Failed to create RDB file: " + rdbFile.getAbsolutePath());
                }
            }

            // 创建AOF文件
            if (!aofFile.exists()) {
                logger.info("Creating AOF file: {}", aofFile.getAbsolutePath());
                if (!aofFile.createNewFile()) {
                    logger.error("Failed to create AOF file: {}", aofFile.getAbsolutePath());
                    throw new RedisException("Failed to create AOF file: " + aofFile.getAbsolutePath());
                }
            }

            // 检查文件权限
            if (!rdbFile.canWrite() || !rdbFile.canRead()) {
                throw new RedisException("Insufficient permissions for RDB file: " + rdbFile.getAbsolutePath());
            }

            if (!aofFile.canWrite() || !aofFile.canRead()) {
                throw new RedisException("Insufficient permissions for AOF file: " + aofFile.getAbsolutePath());
            }

            // 如果使用AOF模式，打开AOF文件
            if (RedisConstants.PERSISTENCE_AOF.equals(config.getPersistenceMode())) {
                aofPersistence.openAofFile(aofFile);
            }
        } catch (Exception e) {
            logger.error("Failed to initialize persistence files", e);
            throw new RedisException("Failed to initialize persistence files", e);
        }
    }

    /**
     * 启动定时持久化任务
     */
    public void startScheduledPersistence(List<RedisDatabase> databases) {
        if (RedisConstants.PERSISTENCE_RDB.equals(config.getPersistenceMode())) {
            // 定时RDB持久化
            int interval = config.getRdbSaveInterval();
            if (interval > 0) {
                logger.info("Scheduled RDB persistence every {} seconds", interval);
                scheduler.scheduleAtFixedRate(() -> {
                    try {
                        logger.info("Starting scheduled RDB persistence");
                        saveRdb(databases);
                        logger.info("Scheduled RDB persistence completed");
                    } catch (Exception e) {
                        logger.error("Scheduled RDB persistence failed", e);
                    }
                }, interval, interval, TimeUnit.SECONDS);
            }
        } else if (RedisConstants.PERSISTENCE_AOF.equals(config.getPersistenceMode())) {
            // 定时AOF刷盘
            if ("everysec".equals(config.getAofFsync())) {
                logger.info("Scheduled AOF fsync every second");
                scheduler.scheduleAtFixedRate(() -> {
                    try {
                        aofPersistence.flushAof();
                    } catch (Exception e) {
                        logger.error("Scheduled AOF fsync failed", e);
                    }
                }, 1, 1, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * 关闭持久化管理器
     */
    public void shutdown(List<RedisDatabase> databases) {
        logger.info("Shutting down persistence manager");

        // 关闭定时任务
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 执行最后一次保存
        try {
            if (RedisConstants.PERSISTENCE_RDB.equals(config.getPersistenceMode())) {
                saveRdb(databases);
            } else if (RedisConstants.PERSISTENCE_AOF.equals(config.getPersistenceMode())) {
                aofPersistence.closeAofFile();
            }
        } catch (Exception e) {
            logger.error("Final persistence failed during shutdown", e);
        }
    }

    /**
     * 加载数据
     *
     * @param databases 数据库列表
     */
    public void loadData(List<RedisDatabase> databases) {
        boolean isReload = Thread.currentThread().getName().contains("redis-db-tasks");
        String operation = isReload ? "重新加载" : "初始加载";

        logger.info("开始{}持久化数据，模式: {}, 文件路径: {}",
                operation,
                config.getPersistenceMode(),
                RedisConstants.PERSISTENCE_RDB.equals(config.getPersistenceMode()) ? rdbFile.getAbsolutePath()
                        : aofFile.getAbsolutePath());

        if (RedisConstants.PERSISTENCE_RDB.equals(config.getPersistenceMode())) {
            loadRdb(databases);
        } else if (RedisConstants.PERSISTENCE_AOF.equals(config.getPersistenceMode())) {
            loadAof(databases);
        }

        // 输出加载后的数据库状态
        int totalKeys = 0;
        for (int i = 0; i < databases.size(); i++) {
            RedisDatabase db = databases.get(i);
            int dbKeys = db.size();
            totalKeys += dbKeys;
            if (dbKeys > 0) {
                logger.info("数据库 {} 加载了 {} 个键", i, dbKeys);
            }
        }
        logger.info("持久化数据{}完成，共 {} 个数据库，{} 个键",
                operation, databases.size(), totalKeys);
    }

    /**
     * 保存数据
     *
     * @param databases 数据库列表
     */
    public void saveData(List<RedisDatabase> databases) {
        if (RedisConstants.PERSISTENCE_RDB.equals(config.getPersistenceMode())) {
            saveRdb(databases);
        } else if (RedisConstants.PERSISTENCE_AOF.equals(config.getPersistenceMode())) {
            saveAof(databases);
        }
    }

    /**
     * 加载RDB文件
     */
    private void loadRdb(List<RedisDatabase> databases) {
        if (!rdbFile.exists() || rdbFile.length() == 0) {
            logger.info("RDB file does not exist or is empty, skipping load");
            return;
        }

        try {
            logger.info("Loading data from RDB file: {}", rdbFile.getAbsolutePath());
            long startTime = System.currentTimeMillis();

            rdbPersistence.load(rdbFile, databases);

            long duration = System.currentTimeMillis() - startTime;
            logger.info("RDB load completed in {} ms", duration);
        } catch (Exception e) {
            logger.error("Failed to load RDB file", e);
            throw new RedisException("Failed to load RDB file", e);
        }
    }

    /**
     * 保存RDB文件
     */
    private void saveRdb(List<RedisDatabase> databases) {
        try {
            logger.info("Saving data to RDB file: {}", rdbFile.getAbsolutePath());
            long startTime = System.currentTimeMillis();

            rdbPersistence.save(databases, rdbFile);

            long duration = System.currentTimeMillis() - startTime;
            logger.info("RDB save completed in {} ms", duration);
        } catch (Exception e) {
            logger.error("Failed to save RDB file", e);
            throw new RedisException("Failed to save RDB file", e);
        }
    }

    /**
     * 加载AOF文件
     */
    private void loadAof(List<RedisDatabase> databases) {
        if (!aofFile.exists() || aofFile.length() == 0) {
            logger.info("AOF file does not exist or is empty, skipping load");
            return;
        }

        try {
            logger.info("Loading data from AOF file: {}", aofFile.getAbsolutePath());
            long startTime = System.currentTimeMillis();

            aofPersistence.load(aofFile, databases);

            long duration = System.currentTimeMillis() - startTime;
            logger.info("AOF load completed in {} ms", duration);
        } catch (Exception e) {
            logger.error("Failed to load AOF file", e);
            throw new RedisException("Failed to load AOF file", e);
        }
    }

    /**
     * 保存AOF文件
     */
    private void saveAof(List<RedisDatabase> databases) {
        try {
            logger.info("Saving data to AOF file: {}", aofFile.getAbsolutePath());
            long startTime = System.currentTimeMillis();

            aofPersistence.save(databases, aofFile);

            long duration = System.currentTimeMillis() - startTime;
            logger.info("AOF save completed in {} ms", duration);
        } catch (Exception e) {
            logger.error("Failed to save AOF file", e);
            throw new RedisException("Failed to save AOF file", e);
        }
    }

    /**
     * 追加命令到AOF文件
     *
     * @param command 要追加的命令
     */
    public void appendCommand(String command, String[] args) {
        if (RedisConstants.PERSISTENCE_AOF.equals(config.getPersistenceMode())) {
            try {
                aofPersistence.appendCommand(command, args);
            } catch (Exception e) {
                logger.error("Failed to append command to AOF file", e);
            }
        }
    }

    /**
     * 获取RDB持久化实现
     */
    public RdbPersistence getRdbPersistence() {
        return rdbPersistence;
    }

    /**
     * 获取AOF持久化实现
     */
    public AofPersistence getAofPersistence() {
        return aofPersistence;
    }
}