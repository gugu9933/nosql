package com.szt.command;

import java.util.Set;

/**
 * 命令执行器接口
 * 负责执行Redis命令
 */
public interface CommandExecutor {

    /**
     * 获取支持的命令集合
     */
    Set<String> getSupportedCommands();

    /**
     * 执行命令
     *
     * @param command 命令名称
     * @param args    命令参数
     * @return 命令执行结果
     */
    String execute(String command, String[] args);

    /**
     * 检查是否支持指定命令
     */
    default boolean supports(String commandName) {
        return getSupportedCommands().contains(commandName.toLowerCase());
    }
}