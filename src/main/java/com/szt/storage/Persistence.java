package com.szt.storage;

import com.szt.core.db.RedisDatabase;

import java.io.File;
import java.util.List;

/**
 * 持久化接口
 * 定义持久化操作的基本方法
 */
public interface Persistence {

    /**
     * 保存数据
     *
     * @param databases 数据库列表
     * @param file      目标文件
     * @throws Exception 持久化异常
     */
    void save(List<RedisDatabase> databases, File file) throws Exception;

    /**
     * 加载数据
     *
     * @param file      源文件
     * @param databases 数据库列表
     * @throws Exception 加载异常
     */
    void load(File file, List<RedisDatabase> databases) throws Exception;
}