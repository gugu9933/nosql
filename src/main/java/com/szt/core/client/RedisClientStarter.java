package com.szt.core.client;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Scanner;

/**
 * Redis客户端启动类
 * 连接到Redis服务器并提供命令行界面
 */
public class RedisClientStarter {

    public static void main(String[] args) throws IOException {
        // 加载配置文件
        Properties props = new Properties();
        try (InputStream input = RedisClientStarter.class.getClassLoader().getResourceAsStream("jadis.properties")) {
            if (input == null) {
                System.out.println("无法找到 jadis.properties 配置文件");
                return;
            }
            props.load(input);
        }

        String host = props.getProperty("host", "127.0.0.1");
        int port = Integer.parseInt(props.getProperty("port", "6380"));
        boolean clusterEnabled = Boolean.parseBoolean(props.getProperty("clusterEnabled", "false"));

        // 如果有命令行参数，覆盖配置文件的设置
        if (args.length >= 1) {
            String[] hostPort = args[0].split(":");
            host = hostPort[0];
            if (hostPort.length > 1) {
                port = Integer.parseInt(hostPort[1]);
            }
        }

        System.out.println("连接到服务器: " + host + ":" + port);
        System.out.println("运行模式: " + (clusterEnabled ? "集群模式" : "单机模式"));

        try {
            Socket client = new Socket(host, port);

            // 使用UTF-8编码处理输入输出
            PrintWriter out = new PrintWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8),
                    true);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));

            // 使用UTF-8编码处理控制台输入
            Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());

            // 读取欢迎消息
            String welcomeMsg = in.readLine();
            System.out.println(welcomeMsg);

            System.out.println("请输入命令（输入'exit'退出）：");
            try {
                while (true) {
                    System.out.print(host + ":" + port + "> ");
                    String input = scanner.nextLine();
                    if ("exit".equalsIgnoreCase(input)) {
                        break;
                    }

                    // 发送命令到服务器
                    out.println(input);
                    out.flush(); // 确保命令被发送

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
        } catch (IOException e) {
            System.err.println("连接服务器失败: " + e.getMessage());
            e.printStackTrace();
        }
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
                return null;
            }
            String value = in.readLine();
            return value != null ? value : "(nil)";
        }

        // 如果响应以'*'开头，这是一个数组
        if (firstLine.startsWith("*")) {
            int count = Integer.parseInt(firstLine.substring(1));
            if (count == -1) {
                return "(empty list or set)";
            }
            if (count == 0) {
                return "[]";
            }

            StringBuilder result = new StringBuilder();
            result.append("[");
            for (int i = 0; i < count; i++) {
                String typeLine = in.readLine();
                if (typeLine.startsWith("$")) {
                    int length = Integer.parseInt(typeLine.substring(1));
                    if (length == -1) {
                        result.append("null");
                    } else {
                        String value = in.readLine();
                        result.append("\"").append(value).append("\"");
                    }
                } else {
                    result.append(typeLine);
                }
                if (i < count - 1) {
                    result.append(", ");
                }
            }
            result.append("]");
            return result.toString();
        }

        // 如果不是特殊格式，直接返回
        return firstLine;
    }
}
