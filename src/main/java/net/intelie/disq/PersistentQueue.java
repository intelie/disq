package net.intelie.disq;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class PersistentQueue<T> implements Closeable {
    private final InternalQueue queue;
    private final SerializerPool<T> pool;

    public PersistentQueue(InternalQueue queue, SerializerPool<T> pool) {
        this.queue = queue;
        this.pool = pool;
    }

    public SerializerPool<T> pool() {
        return pool;
    }

    public RawQueue rawQueue() {
        return queue.rawQueue();
    }

    public ArrayRawQueue fallbackQueue() {
        return queue.fallbackQueue();
    }

    public void setPopPaused(boolean popPaused) {
        queue.setPopPaused(popPaused);
    }

    public void setPushPaused(boolean pushPaused) {
        queue.setPushPaused(pushPaused);
    }

    public void reopen() throws IOException {
        queue.reopen();
    }

    public long bytes() {
        return queue.bytes();
    }

    public long count() {
        return queue.count();
    }

    public long remainingBytes() {
        return queue.remainingBytes();
    }

    public long remainingCount() {
        return queue.remainingCount();
    }

    public void clear() throws IOException {
        queue.clear();
    }

    public void flush() throws IOException {
        queue.flush();
    }

    public T blockingPop(long amount, TimeUnit unit) throws InterruptedException, IOException {
        try (SerializerPool<T>.Slot slot = pool.acquire()) {
            return slot.blockingPop(queue, amount, unit);
        }
    }

    public T blockingPop() throws InterruptedException, IOException {
        try (SerializerPool<T>.Slot slot = pool.acquire()) {
            return slot.blockingPop(queue);
        }

    }

    public boolean blockingPush(T obj, long amount, TimeUnit unit) throws InterruptedException, IOException {
        try (SerializerPool<T>.Slot slot = pool.acquire()) {
            return slot.blockingPush(queue, obj, amount, unit);
        }
    }

    public void blockingPush(T obj) throws InterruptedException, IOException {
        try (SerializerPool<T>.Slot slot = pool.acquire()) {
            slot.blockingPush(queue, obj);
        }
    }

    public T pop() throws IOException {
        try (SerializerPool<T>.Slot slot = pool.acquire()) {
            return slot.pop(queue);
        }
    }

    public boolean push(T obj) throws IOException {
        try (SerializerPool<T>.Slot slot = pool.acquire()) {
            return slot.push(queue, obj);
        }
    }

    public T peek() throws IOException {
        try (SerializerPool<T>.Slot slot = pool.acquire()) {
            return slot.peek(queue);
        }
    }

    public void close() {
        queue.close();
    }
}
