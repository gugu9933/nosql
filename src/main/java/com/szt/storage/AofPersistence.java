package com.szt.storage;

import com.szt.common.exception.RedisException;
import com.szt.config.RedisConfig;
import com.szt.core.data.RedisDataType;
import com.szt.core.data.RedisObject;
import com.szt.core.db.RedisDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * AOF持久化实现
 * 基于命令追加的持久化方式
 */
public class AofPersistence implements Persistence {
    private static final Logger logger = LoggerFactory.getLogger(AofPersistence.class);

    /**
     * 配置信息
     */
    private final RedisConfig config;

    /**
     * AOF文件写入器
     */
    private BufferedWriter writer;

    /**
     * 构造函数
     *
     * @param config 配置信息
     */
    public AofPersistence(RedisConfig config) {
        this.config = config;
    }

    @Override
    public void save(List<RedisDatabase> databases, File file) throws Exception {
        // AOF模式下不需要主动保存，而是实时追加命令
        // 这里实现的是AOF重写功能
        File tempFile = new File(file.getAbsolutePath() + ".tmp");

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile))) {
            // 遍历所有数据库
            for (int i = 0; i < databases.size(); i++) {
                RedisDatabase db = databases.get(i);
                if (db == null) {
                    continue;
                }

                // 写入选择数据库的命令
                bw.write("SELECT " + i);
                bw.newLine();

                // 遍历数据库中的所有键，生成重建命令
                Set<String> keys = db.getKeys();
                for (String key : keys) {
                    RedisObject obj = db.get(key);
                    if (obj == null) {
                        continue;
                    }

                    // 根据不同的数据类型生成不同的命令
                    switch (obj.getType()) {
                        case STRING:
                            String value = obj.getTypedData();
                            bw.write("SET " + key + " " + value);
                            bw.newLine();
                            break;
                        case LIST:
                            List<String> list = obj.getTypedData();
                            for (String item : list) {
                                bw.write("LPUSH " + key + " " + item);
                                bw.newLine();
                            }
                            break;
                        case SET:
                            Set<String> set = obj.getTypedData();
                            for (String item : set) {
                                bw.write("SADD " + key + " " + item);
                                bw.newLine();
                            }
                            break;
                        case HASH:
                            Map<String, String> hash = obj.getTypedData();
                            for (Map.Entry<String, String> entry : hash.entrySet()) {
                                bw.write("HSET " + key + " " + entry.getKey() + " " + entry.getValue());
                                bw.newLine();
                            }
                            break;
                        case ZSET:
                            // ZSET的实现比较复杂，这里简化处理
                            break;
                    }

                    // 如果有过期时间，添加过期命令
                    if (obj.getExpireTime() != null) {
                        long ttl = obj.getExpireTime() - System.currentTimeMillis();
                        if (ttl > 0) {
                            bw.write("PEXPIRE " + key + " " + ttl);
                            bw.newLine();
                        }
                    }
                }
            }

            bw.flush();
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
        if (!file.exists() || file.length() == 0) {
            logger.info("AOF file does not exist or is empty, skipping load");
            return;
        }

        logger.info("开始从AOF文件加载数据: {}", file.getAbsolutePath());
        int commandCount = 0;
        int lineCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            int currentDb = 0;

            while ((line = br.readLine()) != null) {
                lineCount++;
                if (line.trim().isEmpty()) {
                    continue;
                }

                // 解析命令
                String[] parts = line.split("\\s+");
                String cmd = parts[0].toUpperCase();
                logger.debug("正在处理AOF命令: {}", line);

                // 处理SELECT命令
                if ("SELECT".equals(cmd) && parts.length > 1) {
                    try {
                        currentDb = Integer.parseInt(parts[1]);
                        if (currentDb < 0 || currentDb >= databases.size()) {
                            currentDb = 0;
                        }
                        logger.debug("切换到数据库: {}", currentDb);
                    } catch (NumberFormatException e) {
                        // 忽略无效的SELECT命令
                        logger.warn("无效的SELECT命令: {}", line);
                    }
                    continue;
                }

                // 执行命令
                executeCommand(cmd, parts, databases.get(currentDb));
                commandCount++;
            }
        }

        logger.info("AOF文件加载完成，共处理 {} 行，执行 {} 条命令", lineCount, commandCount);
    }

    /**
     * 执行命令
     *
     * @param cmd  命令名称
     * @param args 命令参数
     * @param db   目标数据库
     */
    private void executeCommand(String cmd, String[] args, RedisDatabase db) {
        if (db == null) {
            logger.warn("数据库对象为空，无法执行命令: {}", cmd);
            return;
        }

        String[] cmdArgs = new String[args.length - 1];
        System.arraycopy(args, 1, cmdArgs, 0, cmdArgs.length);

        try {
            switch (cmd) {
                case "SET":
                    if (cmdArgs.length >= 2) {
                        String key = cmdArgs[0];
                        String value = cmdArgs[1];
                        RedisObject obj = RedisObject.createString(value);
                        db.set(key, obj);
                        logger.debug("执行SET命令: key={}, value={}", key, value);
                    } else {
                        logger.warn("SET命令参数不足: {}", String.join(" ", args));
                    }
                    break;
                case "LPUSH":
                    if (cmdArgs.length >= 2) {
                        String key = cmdArgs[0];
                        RedisObject obj = db.get(key);

                        // 如果键不存在或不是列表类型，创建新的列表
                        if (obj == null || obj.getType() != RedisDataType.LIST) {
                            obj = RedisObject.createList();
                            db.set(key, obj);
                        }

                        // 添加元素到列表
                        List<String> list = obj.getTypedData();
                        for (int i = 1; i < cmdArgs.length; i++) {
                            list.add(0, cmdArgs[i]); // 从左侧添加
                        }
                        logger.debug("执行LPUSH命令: key={}, 添加{}个元素", key, cmdArgs.length - 1);
                    } else {
                        logger.warn("LPUSH命令参数不足: {}", String.join(" ", args));
                    }
                    break;
                case "SADD":
                    if (cmdArgs.length >= 2) {
                        String key = cmdArgs[0];
                        RedisObject obj = db.get(key);

                        // 如果键不存在或不是集合类型，创建新的集合
                        if (obj == null || obj.getType() != RedisDataType.SET) {
                            obj = RedisObject.createSet();
                            db.set(key, obj);
                        }

                        // 添加元素到集合
                        Set<String> set = obj.getTypedData();
                        for (int i = 1; i < cmdArgs.length; i++) {
                            set.add(cmdArgs[i]);
                        }
                        logger.debug("执行SADD命令: key={}, 添加{}个元素", key, cmdArgs.length - 1);
                    } else {
                        logger.warn("SADD命令参数不足: {}", String.join(" ", args));
                    }
                    break;
                case "HSET":
                    if (cmdArgs.length >= 3) {
                        String key = cmdArgs[0];
                        String field = cmdArgs[1];
                        String value = cmdArgs[2];

                        RedisObject obj = db.get(key);

                        // 如果键不存在或不是哈希类型，创建新的哈希
                        if (obj == null || obj.getType() != RedisDataType.HASH) {
                            obj = RedisObject.createHash();
                            db.set(key, obj);
                        }

                        // 设置哈希字段
                        Map<String, String> hash = obj.getTypedData();
                        hash.put(field, value);
                        logger.debug("执行HSET命令: key={}, field={}, value={}", key, field, value);
                    } else {
                        logger.warn("HSET命令参数不足: {}", String.join(" ", args));
                    }
                    break;
                case "PEXPIRE":
                    if (cmdArgs.length >= 2) {
                        String key = cmdArgs[0];
                        try {
                            long ttl = Long.parseLong(cmdArgs[1]);
                            boolean result = db.expire(key, ttl);
                            logger.debug("执行PEXPIRE命令: key={}, ttl={}, 结果={}", key, ttl, result);
                        } catch (NumberFormatException e) {
                            logger.warn("无效的过期时间: {}", cmdArgs[1]);
                        }
                    } else {
                        logger.warn("PEXPIRE命令参数不足: {}", String.join(" ", args));
                    }
                    break;
                default:
                    logger.warn("不支持的命令: {}", cmd);
            }
        } catch (Exception e) {
            logger.error("执行AOF命令出错: {} {}", cmd, String.join(" ", cmdArgs), e);
        }
    }

    /**
     * 追加命令到AOF文件
     *
     * @param command 命令名称
     * @param args    命令参数
     * @throws IOException IO异常
     */
    public synchronized void appendCommand(String command, String[] args) throws IOException {
        if (writer == null) {
            return;
        }

        // 构造命令字符串
        StringBuilder sb = new StringBuilder();
        sb.append(command);

        for (String arg : args) {
            sb.append(" ").append(arg);
        }

        // 写入命令
        writer.write(sb.toString());
        writer.newLine();

        // 根据配置决定是否立即刷新
        if ("always".equals(config.getAofFsync())) {
            writer.flush();
        }
    }

    /**
     * 打开AOF文件
     *
     * @param file AOF文件
     * @throws IOException IO异常
     */
    public void openAofFile(File file) throws IOException {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                // 忽略关闭异常
            }
        }

        writer = new BufferedWriter(new FileWriter(file, true));
        logger.info("Opened AOF file for writing: {}", file.getAbsolutePath());
    }

    /**
     * 关闭AOF文件
     */
    public void closeAofFile() {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
                logger.info("Closed AOF file");
            } catch (IOException e) {
                logger.error("Error closing AOF file", e);
            } finally {
                writer = null;
            }
        }
    }

    /**
     * 刷新AOF文件
     */
    public void flushAof() {
        if (writer != null) {
            try {
                writer.flush();
            } catch (IOException e) {
                throw new RedisException("Failed to flush AOF file", e);
            }
        }
    }
}