package net.intelie.disq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PersistentQueue<T> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistentQueue.class);

    private static final long MAX_WAIT = 10_000_000_000L;

    private final ObjectPool<Slot> pool;
    private final ArrayRawQueue fallback;
    private final RawQueue queue;
    private final SerializerFactory<T> serializerFactory;
    private final int initialBufferCapacity;
    private final int maxBufferCapacity;
    private final Lock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    private boolean popPaused, pushPaused;

    public PersistentQueue(RawQueue queue, SerializerFactory<T> serializer, int initialBufferCapacity, int maxBufferCapacity) {
        this(queue, serializer, initialBufferCapacity, maxBufferCapacity, 0);
    }

    public PersistentQueue(RawQueue queue, SerializerFactory<T> serializer, int initialBufferCapacity, int maxBufferCapacity, int fallbackBufferCapacity) {
        this.fallback = new ArrayRawQueue(fallbackBufferCapacity, true);
        this.queue = queue;
        this.serializerFactory = serializer;
        this.initialBufferCapacity = initialBufferCapacity;
        this.maxBufferCapacity = maxBufferCapacity;
        this.pool = new ObjectPool<>(Slot::new);
    }

    public RawQueue rawQueue() {
        return queue;
    }

    public ArrayRawQueue fallbackQueue() {
        return fallback;
    }

    public void setPopPaused(boolean popPaused) {
        lock.lock();
        try {
            this.popPaused = popPaused;
            this.notFull.signalAll();
            this.notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void setPushPaused(boolean pushPaused) {
        lock.lock();
        try {
            this.pushPaused = pushPaused;
            this.notFull.signalAll();
            this.notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void reopen() throws IOException {
        queue.reopen();
        fallback.reopen();
    }

    public long bytes() {
        return queue.bytes() + fallback.bytes();
    }

    public long count() {
        return queue.count() + fallback.count();
    }

    public long remainingBytes() {
        return queue.remainingBytes();
    }

    public long remainingCount() {
        return queue.remainingCount();
    }

    public void clear() throws IOException {
        queue.clear();
        fallback.clear();
    }

    public void flush() throws IOException {
        queue.flush();
        fallback.flush();
    }

    public T blockingPop(long amount, TimeUnit unit) throws InterruptedException, IOException {
        try (ObjectPool<Slot>.Ref ref = pool.acquire()) {
            Slot slot = ref.obj();
            lock.lockInterruptibly();
            try {
                long target = System.nanoTime() + unit.toNanos(amount);
                while (!notifyingPop(slot.buffer)) {
                    long wait = Math.min(MAX_WAIT, target - System.nanoTime());
                    if (wait <= 0) return null;
                    notEmpty.awaitNanos(wait);
                }
            } finally {
                lock.unlock();
            }
            return safeDeserialize(slot.buffer, slot.serializer);
        }
    }

    public T blockingPop() throws InterruptedException, IOException {
        try (ObjectPool<Slot>.Ref ref = pool.acquire()) {
            Slot slot = ref.obj();
            lock.lockInterruptibly();
            try {
                while (!notifyingPop(slot.buffer))
                    notEmpty.awaitNanos(MAX_WAIT);
            } finally {
                lock.unlock();
            }
            return safeDeserialize(slot.buffer, slot.serializer);
        }
    }

    public boolean blockingPush(T obj, long amount, TimeUnit unit) throws InterruptedException, IOException {
        try (ObjectPool<Slot>.Ref ref = pool.acquire()) {
            Slot slot = ref.obj();
            serialize(obj, slot.buffer, slot.serializer);
            lock.lockInterruptibly();
            try {
                long target = System.nanoTime() + unit.toNanos(amount);
                while (!notifyingPush(slot.buffer)) {
                    long wait = Math.min(MAX_WAIT, target - System.nanoTime());
                    if (wait <= 0) return false;

                    notFull.awaitNanos(wait);
                }
            } finally {
                lock.unlock();
            }
            return true;
        }
    }

    public boolean blockingPush(T obj) throws InterruptedException, IOException {
        try (ObjectPool<Slot>.Ref ref = pool.acquire()) {
            Slot slot = ref.obj();
            serialize(obj, slot.buffer, slot.serializer);
            lock.lockInterruptibly();
            try {
                while (!notifyingPush(slot.buffer))
                    notFull.awaitNanos(MAX_WAIT);
            } finally {
                lock.unlock();
            }
            return true;
        }
    }

    public T pop() throws IOException {
        try (ObjectPool<Slot>.Ref ref = pool.acquire()) {
            Slot slot = ref.obj();
            lock.lock();
            try {
                if (!notifyingPop(slot.buffer)) return null;
            } finally {
                lock.unlock();
            }
            return safeDeserialize(slot.buffer, slot.serializer);
        }
    }

    private T safeDeserialize(Buffer buffer, Serializer<T> serializer) throws IOException {
        try {
            return serializer.deserialize(buffer);
        } catch (Throwable e) {
            queue.notifyFailedRead();
            throw e;
        }
    }

    public boolean push(T obj) throws IOException {
        try (ObjectPool<Slot>.Ref ref = pool.acquire()) {
            Slot slot = ref.obj();
            serialize(obj, slot.buffer, slot.serializer);
            lock.lock();
            try {
                if (!notifyingPush(slot.buffer)) return false;
            } finally {
                lock.unlock();
            }
            return true;
        }
    }

    private boolean notifyingPop(Buffer buffer) {
        if (popPaused) return false;
        if (!innerPop(buffer))
            return false;
        else {
            notFull.signalAll();
            return true;
        }
    }

    private boolean notifyingPush(Buffer buffer) {
        if (pushPaused) return false;
        if (!innerPush(buffer))
            return false;
        else {
            notEmpty.signal();
            return true;
        }
    }

    private void serialize(T obj, Buffer buffer, Serializer<T> serializer) throws IOException {
        if (obj == null)
            throw new NullPointerException("Null elements not allowed in persistent queue.");

        buffer.setCount(0, false);
        serializer.serialize(buffer, obj);
    }

    public T peek() throws IOException {
        try (ObjectPool<Slot>.Ref ref = pool.acquire()) {
            Slot slot = ref.obj();
            if (!innerPeek(slot.buffer))
                return null;
            return safeDeserialize(slot.buffer, slot.serializer);
        }
    }

    private boolean innerPeek(Buffer buffer) {
        if (popPaused) return false;
        if (fallback.peek(buffer)) return true;

        try {
            return queue.peek(buffer);
        } catch (IOException e) {
            LOGGER.info("Error peeking", e);
            return false;
        }
    }

    private boolean innerPop(Buffer buffer) {
        if (fallback.pop(buffer)) return true;

        try {
            return queue.pop(buffer);
        } catch (IOException e) {
            LOGGER.info("Error popping", e);
            return false;
        }
    }

    private boolean innerPush(Buffer buffer) {
        try {
            return queue.push(buffer);
        } catch (IOException e) {
            LOGGER.info("Error pushing", e);
            return fallback.push(buffer);
        }
    }

    public void close() {
        queue.close();
    }

    private class Slot {
        private final Buffer buffer = new Buffer(initialBufferCapacity, maxBufferCapacity);
        private final Serializer<T> serializer = serializerFactory.create();
    }
}
