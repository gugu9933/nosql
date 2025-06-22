package com.szt.command;

import com.szt.core.db.RedisDatabase;
import lombok.Data;

/**
 * Redis命令基类
 */
@Data
public class RedisCommand {

    /**
     * 命令名称
     */
    private final String name;

    /**
     * 命令参数
     */
    private final String[] args;

    /**
     * 目标数据库
     */
    private final RedisDatabase database;

    /**
     * 客户端连接ID
     */
    private final String clientId;

    public RedisCommand(String name, String[] args, RedisDatabase database, String clientId) {
        this.name = name.toLowerCase();
        this.args = args;
        this.database = database;
        this.clientId = clientId;
    }

    /**
     * 获取参数数量
     */
    public int getArgCount() {
        return args != null ? args.length : 0;
    }

    /**
     * 获取指定位置的参数
     */
    public String getArg(int index) {
        if (args == null || index < 0 || index >= args.length) {
            return null;
        }
        return args[index];
    }

    /**
     * 检查参数数量是否符合要求
     */
    public boolean checkArgCount(int required) {
        return getArgCount() >= required;
    }
}