package com.szt.event;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 默认的键事件发布器实现
 */
public class DefaultKeyEventPublisher implements KeyEventPublisher {

    private final Set<KeyEventListener> listeners = new CopyOnWriteArraySet<>();

    @Override
    public void publishEvent(KeyEvent event) {
        for (KeyEventListener listener : listeners) {
            try {
                listener.onEvent(event);
            } catch (Exception e) {
                // 记录异常但不影响其他监听器
                e.printStackTrace();
            }
        }
    }

    @Override
    public void addEventListener(KeyEventListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    @Override
    public void removeEventListener(KeyEventListener listener) {
        if (listener != null) {
            listeners.remove(listener);
        }
    }
}