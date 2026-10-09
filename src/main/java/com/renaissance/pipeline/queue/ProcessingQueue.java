package com.renaissance.pipeline.queue;

import com.renaissance.pipeline.model.Message;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Ограниченная очередь между FileReader и ProcessingWorkers (архитектура §3 / §7.3).
 * Полная очередь → {@link #put} блокируется (backpressure на Reader).
 */
public final class ProcessingQueue {

    private final BlockingQueue<Message> delegate;

    /** Очередь ёмкости {@code capacity}. */
    public ProcessingQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got " + capacity);
        }
        this.delegate = new ArrayBlockingQueue<>(capacity);
    }

    /** Положить сообщение (блокируется при полной очереди). */
    public void put(Message message) throws InterruptedException {
        delegate.put(message);
    }

    /** Забрать сообщение (блокируется при пустой очереди). */
    public Message take() throws InterruptedException {
        return delegate.take();
    }

    /** Текущий размер очереди. */
    public int size() {
        return delegate.size();
    }

    /** Свободная ёмкость. */
    public int remainingCapacity() {
        return delegate.remainingCapacity();
    }

    /** Делегат {@link BlockingQueue} для API, которым он нужен. */
    public BlockingQueue<Message> asBlockingQueue() {
        return delegate;
    }
}
