package com.szt.event;

/**
 * 键事件监听器
 * 用于处理键相关的事件
 */
public interface KeyEventListener {

    /**
     * 处理事件
     * 
     * @param event 键事件
     */
    void onEvent(KeyEvent event);
}