package com.szt.storage;

import com.szt.common.exception.RedisException;
import com.szt.config.RedisConfig;
import com.szt.core.data.RedisObject;
import com.szt.core.db.RedisDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * RDB持久化实现
 * 基于快照的持久化方式
 */
public class RdbPersistence implements Persistence {
    private static final Logger logger = LoggerFactory.getLogger(RdbPersistence.class);
    private static final String REDIS_MAGIC_STRING = "REDIS0001";

    /**
     * 配置信息
     */
    private final RedisConfig config;

    /**
     * 构造函数
     *
     * @param config 配置信息
     */
    public RdbPersistence(RedisConfig config) {
        this.config = config;
    }

    @Override
    public void save(List<RedisDatabase> databases, File file) throws Exception {
        logger.info("开始执行RDB持久化存储操作");
        long startTime = System.currentTimeMillis();

        File tempFile = new File(file.getAbsolutePath() + ".tmp");

        try (FileOutputStream fos = new FileOutputStream(tempFile);
                OutputStream os = config.isRdbCompression() ? new GZIPOutputStream(fos) : fos;
                ObjectOutputStream oos = new ObjectOutputStream(os)) {

            // 写入文件头
            oos.writeBytes(REDIS_MAGIC_STRING); // 魔数和版本号

            // 写入数据库数量
            oos.writeInt(databases.size());

            // 写入每个数据库
            int totalKeys = 0;
            for (int i = 0; i < databases.size(); i++) {
                RedisDatabase db = databases.get(i);
                if (db == null) {
                    // 写入空数据库
                    oos.writeInt(i);
                    oos.writeInt(0);
                    continue;
                }

                // 写入数据库索引
                oos.writeInt(i);

                // 获取所有键值对
                Map<String, RedisObject> data = db.getAllData();
                int keyCount = data.size();
                totalKeys += keyCount;

                // 写入键值对数量
                oos.writeInt(keyCount);

                // 写入每个键值对
                for (Map.Entry<String, RedisObject> entry : data.entrySet()) {
                    String key = entry.getKey();
                    RedisObject value = entry.getValue();

                    // 写入键
                    oos.writeUTF(key);

                    // 写入值
                    oos.writeObject(value);
                }
            }

            // 写入文件尾
            oos.writeByte(0xFF);
            oos.flush();

            long endTime = System.currentTimeMillis();
            logger.info("RDB持久化存储完成，共写入 {} 个数据库，{} 个键，耗时：{}ms",
                    databases.size(), totalKeys, (endTime - startTime));
        }

        // 原子性替换文件
        if (!tempFile.renameTo(file)) {
            if (!file.delete() || !tempFile.renameTo(file)) {
                throw new RedisException("Failed to rename temp file to target file");
            }
        }
    }

    @Override
    public void load(File file, List<RedisDatabase> databases) throws Exception {
        if (!file.exists()) {
            logger.info("RDB文件不存在，将创建新文件");
            // 创建一个空的RDB文件
            save(databases, file);
            return;
        }

        if (file.length() == 0) {
            logger.info("RDB文件为空，将初始化新文件");
            // 创建一个空的RDB文件
            save(databases, file);
            return;
        }

        logger.info("开始从RDB文件加载数据");
        long startTime = System.currentTimeMillis();

        // 检查文件是否为有效的RDB文件
        if (!isValidRdbFile(file)) {
            logger.warn("文件不是有效的RDB格式，将创建新的RDB文件");
            // 备份无效文件
            File backupFile = new File(file.getAbsolutePath() + ".bak." + System.currentTimeMillis());
            if (file.renameTo(backupFile)) {
                logger.info("已将无效的RDB文件备份为: {}", backupFile.getAbsolutePath());
            }

            // 创建一个空的RDB文件
            save(databases, file);
            return;
        }

        try {
            // 先尝试使用GZIP读取
            loadWithCompression(file, databases, true);
        } catch (Exception e) {
            logger.warn("使用GZIP解压失败，尝试不使用压缩读取", e);
            try {
                // 如果失败，尝试不使用压缩读取
                loadWithCompression(file, databases, false);
            } catch (Exception ex) {
                logger.error("无法读取RDB文件，可能文件已损坏", ex);
                throw new RedisException("Failed to load RDB file, file may be corrupted");
            }
        }

        long endTime = System.currentTimeMillis();
        logger.info("RDB数据加载完成，耗时：{}ms", (endTime - startTime));
    }

    /**
     * 检查文件是否为有效的RDB文件
     * 
     * @param file 要检查的文件
     * @return 是否为有效的RDB文件
     */
    private boolean isValidRdbFile(File file) {
        // 尝试读取文件头部以验证格式
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] header = new byte[8];
            int bytesRead = fis.read(header);

            if (bytesRead < 8) {
                return false;
            }

            String headerStr = new String(header);
            return headerStr.startsWith("REDIS");
        } catch (Exception e) {
            logger.warn("检查RDB文件格式时出错", e);
            return false;
        }
    }

    /**
     * 使用指定的压缩方式加载数据
     *
     * @param file           源文件
     * @param databases      数据库列表
     * @param useCompression 是否使用压缩
     * @throws Exception 加载异常
     */
    private void loadWithCompression(File file, List<RedisDatabase> databases, boolean useCompression)
            throws Exception {
        try (FileInputStream fis = new FileInputStream(file);
                InputStream is = useCompression ? new GZIPInputStream(fis) : fis;
                ObjectInputStream ois = new ObjectInputStream(is)) {

            // 读取文件头
            byte[] header = new byte[8];
            int bytesRead = ois.read(header);

            if (bytesRead < 8) {
                throw new RedisException("RDB file too short");
            }

            String headerStr = new String(header);
            if (!headerStr.startsWith("REDIS")) {
                throw new RedisException("Invalid RDB file format");
            }

            try {
                // 读取数据库数量
                int dbCount = ois.readInt();

                if (dbCount < 0 || dbCount > 100) {
                    throw new RedisException("Invalid database count in RDB file: " + dbCount);
                }

                // 确保数据库列表大小正确
                while (databases.size() < dbCount) {
                    databases.add(null);
                }

                // 读取每个数据库
                int totalKeys = 0;
                for (int i = 0; i < dbCount; i++) {
                    // 读取数据库索引
                    int dbIndex = ois.readInt();
                    if (dbIndex < 0 || dbIndex >= databases.size()) {
                        logger.warn("Invalid database index in RDB file: {}", dbIndex);
                        continue;
                    }

                    // 读取键值对数量
                    int keyCount = ois.readInt();
                    if (keyCount < 0) {
                        logger.warn("Invalid key count in RDB file: {}", keyCount);
                        continue;
                    }

                    totalKeys += keyCount;

                    // 获取或创建数据库
                    RedisDatabase db = databases.get(dbIndex);
                    if (db == null) {
                        continue;
                    }

                    // 清空数据库
                    db.clear();

                    // 读取每个键值对
                    for (int j = 0; j < keyCount; j++) {
                        try {
                            // 读取键
                            String key = ois.readUTF();

                            // 读取值
                            RedisObject value = (RedisObject) ois.readObject();

                            if (key != null && value != null) {
                                // 存储数据
                                db.set(key, value);
                            }
                        } catch (Exception e) {
                            logger.warn("Error reading key-value pair from RDB file", e);
                            // 继续读取下一个键值对
                        }
                    }
                }

                logger.info("从RDB文件加载了 {} 个数据库，{} 个键", dbCount, totalKeys);
            } catch (EOFException e) {
                throw new RedisException("Unexpected end of RDB file", e);
            }
        }
    }
}