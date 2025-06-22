package com.szt.core.client;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * 从节点客户端启动类
 * 默认连接到从节点，用于测试只读功能
 */
public class RedisSlaveClientStarter {

    public static void main(String[] args) throws IOException {
        String host = "127.0.0.1";
        int port = 6381; // 默认连接第一个从节点

        // 如果有命令行参数，解析主机和端口
        if (args.length >= 1) {
            if ("slave1".equalsIgnoreCase(args[0])) {
                port = 6381; // 从节点1
            } else if ("slave2".equalsIgnoreCase(args[0])) {
                port = 6381; // 从节点2
            } else {
                String[] hostPort = args[0].split(":");
                host = hostPort[0];
                if (hostPort.length > 1) {
                    port = Integer.parseInt(hostPort[1]);
                }
            }
        }

        System.out.println("连接到Redis从节点: " + host + ":" + port);
        System.out.println("注意：从节点只支持读操作，写操作将被拒绝");

        Socket client = new Socket(host, port);

        // 使用UTF-8编码处理输入输出
        PrintWriter out = new PrintWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8),
                true);
        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));

        // 使用UTF-8编码处理控制台输入
        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());

        // 读取欢迎消息
        String welcomeMsg = in.readLine();
        System.out.println(welcomeMsg);

        System.out.println("请输入Redis命令（输入'exit'退出）：");
        System.out.println("支持的只读命令包括：get, exists, type, ttl, keys, lrange, smembers, zrange, hget, hgetall等");

        // 发送READONLY命令，告知服务器这是一个只读连接
        out.println("READONLY");
        String readOnlyResponse = readResponse(in);
        System.out.println("设置只读模式: " + readOnlyResponse);

        try {
            while (true) {
                System.out.print(host + ":" + port + " (只读)> ");
                String input = scanner.nextLine();
                if ("exit".equalsIgnoreCase(input)) {
                    break;
                }

                // 检查是否是写命令
                String cmd = input.trim().split("\\s+")[0].toLowerCase();
                if (isWriteCommand(cmd)) {
                    System.out.println("错误: 从节点不支持写操作 '" + cmd + "'");
                    continue;
                }

                // 发送命令到服务器
                out.println(input);

                // 读取服务器响应
                String response = readResponse(in);
                System.out.println(response);
            }
        } finally {
            // 关闭资源
            scanner.close();
            out.close();
            in.close();
            client.close();
            System.out.println("已断开连接");
        }
    }

    /**
     * 判断命令是否为写命令
     */
    private static boolean isWriteCommand(String cmd) {
        String[] writeCommands = {
                "set", "del", "append", "incr", "decr", "rpush", "lpush", "sadd", "srem",
                "zadd", "zrem", "hset", "hdel", "expire", "rename", "flushdb", "flushall"
        };

        for (String writeCmd : writeCommands) {
            if (writeCmd.equalsIgnoreCase(cmd)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 读取服务器响应
     * 处理可能的多行响应
     */
    private static String readResponse(BufferedReader in) throws IOException {
        String firstLine = in.readLine();
        if (firstLine == null) {
            return "连接已关闭";
        }

        // 如果响应以'+'开头，这是一个简单字符串
        if (firstLine.startsWith("+")) {
            return firstLine.substring(1);
        }

        // 如果响应以'-'开头，这是一个错误
        if (firstLine.startsWith("-")) {
            return "错误: " + firstLine.substring(1);
        }

        // 如果响应以':'开头，这是一个整数
        if (firstLine.startsWith(":")) {
            return firstLine.substring(1);
        }

        // 如果响应以'$'开头，这是一个批量字符串
        if (firstLine.startsWith("$")) {
            int length = Integer.parseInt(firstLine.substring(1));
            if (length == -1) {
                return "(nil)";
            }

            // 对于简单实现，我们只读取一行数据
            return in.readLine();
        }

        // 如果响应以'*'开头，这是一个数组
        if (firstLine.startsWith("*")) {
            int count = Integer.parseInt(firstLine.substring(1));
            if (count == -1) {
                return "(empty list or set)";
            }

            // 对于简单实现，我们假设数组元素都是单行的
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < count; i++) {
                String typeLine = in.readLine();
                if (typeLine.startsWith("$")) {
                    int length = Integer.parseInt(typeLine.substring(1));
                    if (length == -1) {
                        result.append(i + 1).append(") (nil)\n");
                    } else {
                        String value = in.readLine();
                        result.append(i + 1).append(") \"").append(value).append("\"\n");
                    }
                } else {
                    result.append(i + 1).append(") ").append(typeLine).append("\n");
                }
            }
            return result.toString();
        }

        // 如果不是特殊格式，直接返回
        return firstLine;
    }
}