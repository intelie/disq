package net.intelie.disq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PersistentQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistentQueue.class);

    private static final long MAX_WAIT = 10_000_000_000L;

    private final ArrayRawQueue fallback;
    private final RawQueue queue;
    private final Lock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    private boolean popPaused, pushPaused;

    public PersistentQueue(RawQueue queue) {
        this(queue, 0);
    }

    public PersistentQueue(RawQueue queue, int fallbackBufferCapacity) {
        this.fallback = new ArrayRawQueue(fallbackBufferCapacity, true);
        this.queue = queue;
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

    public boolean blockingPop(Buffer buffer, long amount, TimeUnit unit) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            long target = System.nanoTime() + unit.toNanos(amount);
            while (!notifyingPop(buffer)) {
                long wait = Math.min(MAX_WAIT, target - System.nanoTime());
                if (wait <= 0) return false;
                notEmpty.awaitNanos(wait);
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    public void blockingPop(Buffer buffer) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (!notifyingPop(buffer))
                notEmpty.awaitNanos(MAX_WAIT);
        } finally {
            lock.unlock();
        }
    }

    public boolean blockingPush(Buffer buffer, long amount, TimeUnit unit) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            long target = System.nanoTime() + unit.toNanos(amount);
            while (!notifyingPush(buffer)) {
                long wait = Math.min(MAX_WAIT, target - System.nanoTime());
                if (wait <= 0) return false;

                notFull.awaitNanos(wait);
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    public void blockingPush(Buffer buffer) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (!notifyingPush(buffer))
                notFull.awaitNanos(MAX_WAIT);
        } finally {
            lock.unlock();
        }
    }

    public boolean pop(Buffer buffer) {
        lock.lock();
        boolean answer;
        try {
            answer = notifyingPop(buffer);
        } finally {
            lock.unlock();
        }
        return answer;
    }

    public boolean push(Buffer buffer) {
        lock.lock();
        boolean answer;
        try {
            answer = notifyingPush(buffer);
        } finally {
            lock.unlock();
        }
        return answer;
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

    public boolean peek(Buffer buffer) {
        if (popPaused) return false;

        try {
            if (fallback.peek(buffer)) return true;
            return queue.peek(buffer);
        } catch (IOException e) {
            LOGGER.info("Error peeking", e);
            return false;
        }
    }

    private boolean innerPop(Buffer buffer) {

        try {
            if (fallback.pop(buffer)) return true;
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
}
