package com.szt.common.exception;

/**
 * Redis命令执行异常
 * 处理命令语法错误和执行错误
 */
public class RedisCommandException extends RedisException {

    private static final long serialVersionUID = 1L;

    public RedisCommandException(String message) {
        super(String.format("Command Error: %s", message));
    }

    public RedisCommandException(String message, Throwable cause) {
        super(String.format("Command Error: %s", message), cause);
    }
}