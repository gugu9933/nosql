package com.szt.common.exception;

/**
 * Redis配置异常
 * 处理配置加载和解析错误
 */
public class RedisConfigException extends RedisException {

    private static final long serialVersionUID = 1L;

    public RedisConfigException(String message) {
        super(String.format("Configuration Error: %s", message));
    }

    public RedisConfigException(String message, Throwable cause) {
        super(String.format("Configuration Error: %s", message), cause);
    }
}