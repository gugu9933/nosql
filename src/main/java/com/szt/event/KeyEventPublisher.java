package com.szt.event;

/**
 * 键事件发布器
 * 负责发布键相关的事件
 */
public interface KeyEventPublisher {

    /**
     * 发布事件
     * 
     * @param event 键事件
     */
    void publishEvent(KeyEvent event);

    /**
     * 添加事件监听器
     * 
     * @param listener 事件监听器
     */
    void addEventListener(KeyEventListener listener);

    /**
     * 移除事件监听器
     * 
     * @param listener 事件监听器
     */
    void removeEventListener(KeyEventListener listener);
}