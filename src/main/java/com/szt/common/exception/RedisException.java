package com.szt.common.exception;

/**
 * Redis基础异常类
 * 所有Redis相关异常的基类
 */
public class RedisException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RedisException() {
        super();
    }

    public RedisException(String message) {
        super(String.format("Redis Exception: %s", message));
    }

    public RedisException(String message, Throwable cause) {
        super(String.format("Redis Exception: %s", message), cause);
    }

    public RedisException(Throwable cause) {
        super(cause);
    }

    protected RedisException(String message, Object... args) {
        this(String.format(message, args));
    }
} 