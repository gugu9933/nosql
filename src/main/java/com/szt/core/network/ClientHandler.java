package com.szt.core.network;

import com.szt.command.CommandExecutor;
import com.szt.core.db.DatabaseManager;
import com.szt.core.server.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

/**
 * 客户端处理器
 * 负责处理单个客户端连接和命令
 */
public class ClientHandler {
    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

    /**
     * 需要记录到AOF的命令集合（会修改数据的命令）
     */
    private static final Set<String> WRITE_COMMANDS = new HashSet<>(Arrays.asList(
            "SET", "GETSET", "APPEND", "INCR", "INCRBY", "DECR", "DECRBY", "SETEX", "SETNX", "MSET",
            "LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LTRIM",
            "SADD", "SREM", "SPOP", "SMOVE",
            "HSET", "HMSET", "HDEL",
            "ZADD", "ZREM", "ZINCRBY", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
            "DEL", "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "FLUSHDB", "FLUSHALL"));

    /**
     * 客户端Socket
     */
    private final Socket clientSocket;

    /**
     * 命令执行器
     */
    private final CommandExecutor commandExecutor;

    /**
     * 输入流
     */
    private BufferedReader in;

    /**
     * 输出流
     */
    private PrintWriter out;

    /**
     * 是否正在运行
     */
    private boolean running;

    /**
     * 构造函数
     *
     * @param clientSocket    客户端Socket
     * @param commandExecutor 命令执行器
     */
    public ClientHandler(Socket clientSocket, CommandExecutor commandExecutor) {
        this.clientSocket = clientSocket;
        this.commandExecutor = commandExecutor;
    }

    /**
     * 处理客户端请求
     */
    public void handle() {
        try {
            // 初始化流
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            out = new PrintWriter(clientSocket.getOutputStream(), true);

            // 发送欢迎消息
            out.println("+OK Welcome to Java-Redis Server");

            running = true;
            while (running) {
                // 读取命令
                String commandLine = in.readLine();
                if (commandLine == null) {
                    break;
                }

                // 处理命令
                handleCommand(commandLine);
            }
        } catch (IOException e) {
            logger.error("Error handling client connection", e);
        } finally {
            close();
        }
    }

    /**
     * 处理命令
     *
     * @param commandLine 命令行
     */
    private void handleCommand(String commandLine) {
        try {
            logger.debug("收到命令行: '{}'", commandLine);

            // 解析命令（处理引号内的空格）
            String[] parts = parseCommandLine(commandLine.trim());

            if (parts.length == 0) {
                out.println("-ERR no command specified");
                return;
            }

            // 处理退出命令
            if ("exit".equalsIgnoreCase(parts[0]) || "quit".equalsIgnoreCase(parts[0])) {
                running = false;
                out.println("+OK bye");
                return;
            }

            String command = parts[0].toUpperCase();
            String[] args = new String[parts.length - 1];
            System.arraycopy(parts, 1, args, 0, args.length);

            logger.debug("解析后的命令: {}, 参数: {}", command, Arrays.toString(args));

            // 执行命令
            String result = commandExecutor.execute(command, args);
            logger.debug("命令执行结果: {}", result);

            // 如果是写命令，记录到AOF文件
            if (isWriteCommand(command)) {
                try {
                    DatabaseManager dbManager = RedisServer.getDatabaseManager();
                    if (dbManager != null) {
                        dbManager.appendCommandToAof(command, args);
                        logger.debug("Command recorded to AOF: {} {}", command, Arrays.toString(args));
                    }
                } catch (Exception e) {
                    logger.error("Failed to record command to AOF: {}", commandLine, e);
                }
            }

            // 格式化响应
            formatAndSendResponse(command, result);

            // 确保输出立即刷新
            out.flush();
        } catch (Exception e) {
            String errorMsg = e.getMessage();
            if (errorMsg == null || errorMsg.isEmpty()) {
                errorMsg = "internal error";
            }
            out.println("-ERR Redis Exception: " + errorMsg);
            out.flush();
            logger.error("Error executing command: {}", commandLine, e);
        }
    }

    /**
     * 解析命令行，正确处理引号内的空格
     * 
     * @param line 命令行
     * @return 解析后的命令和参数数组
     */
    private String[] parseCommandLine(String line) {
        if (line == null || line.isEmpty()) {
            return new String[0];
        }

        List<String> tokens = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean inSingleQuotes = false;
        boolean inDoubleQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '\'' && !inDoubleQuotes) {
                inSingleQuotes = !inSingleQuotes;
                // 保留引号
                currentToken.append(c);
            } else if (c == '\"' && !inSingleQuotes) {
                inDoubleQuotes = !inDoubleQuotes;
                // 保留引号
                currentToken.append(c);
            } else if (Character.isWhitespace(c) && !inSingleQuotes && !inDoubleQuotes) {
                // 空格在引号外，作为分隔符
                if (currentToken.length() > 0) {
                    tokens.add(currentToken.toString());
                    currentToken = new StringBuilder();
                }
            } else {
                // 其他字符或引号内的空格
                currentToken.append(c);
            }
        }

        // 添加最后一个token
        if (currentToken.length() > 0) {
            tokens.add(currentToken.toString());
        }

        return tokens.toArray(new String[0]);
    }

    /**
     * 判断是否为写命令（需要记录到AOF的命令）
     * 
     * @param command 命令名称
     * @return 是否为写命令
     */
    private boolean isWriteCommand(String command) {
        return WRITE_COMMANDS.contains(command);
    }

    /**
     * 格式化并发送响应
     */
    private void formatAndSendResponse(String command, String result) {
        if (result == null) {
            out.println("(nil)"); // Redis Null Bulk String
            return;
        }

        // 处理Redis协议格式的响应
        if (result.startsWith("+")) {
            // 简单字符串回复
            out.println(result.substring(1).replace("\r\n", ""));
        } else if (result.startsWith("-")) {
            // 错误回复
            out.println(result.substring(1).replace("\r\n", ""));
        } else if (result.startsWith(":")) {
            // 整数回复
            out.println(result.substring(1).replace("\r\n", ""));
        } else if (result.startsWith("$")) {
            // 批量字符串回复
            String[] parts = result.split("\r\n", 2);
            if (parts.length > 1) {
                if (parts[0].equals("$-1")) {
                    out.println("(nil)");
                } else {
                    out.println(parts[1].replace("\r\n", ""));
                }
            } else {
                out.println(result);
            }
        } else if (result.startsWith("*")) {
            // 多行字符串回复，保持原格式
            out.println(result);
        } else {
            // 不是Redis协议格式，直接输出
            out.println(result);
        }
    }

    /**
     * 关闭连接
     */
    private void close() {
        running = false;
        try {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
            if (clientSocket != null && !clientSocket.isClosed()) {
                clientSocket.close();
            }
        } catch (IOException e) {
            logger.error("Error closing client connection", e);
        }
    }
}