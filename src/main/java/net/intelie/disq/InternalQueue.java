package net.intelie.disq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class InternalQueue implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(InternalQueue.class);

    private static final long MAX_WAIT = TimeUnit.SECONDS.toNanos(10);

    private final ArrayRawQueue fallback;
    private final RawQueue queue;
    private final RawQueue original;

    private boolean popPaused, pushPaused;

    public InternalQueue(RawQueue queue) {
        this(queue, 0);
    }

    public InternalQueue(RawQueue queue, int fallbackBufferCapacity) {
        this.fallback = new ArrayRawQueue(fallbackBufferCapacity, true);
        this.original = queue;
        this.queue = new LenientRawQueue(queue);
    }

    public RawQueue rawQueue() {
        return original;
    }

    public ArrayRawQueue fallbackQueue() {
        return fallback;
    }

    public synchronized void setPopPaused(boolean popPaused) {
        this.popPaused = popPaused;
        notifyAll();
    }

    public synchronized void setPushPaused(boolean pushPaused) {
        this.pushPaused = pushPaused;
        notifyAll();
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

    public synchronized boolean blockingPop(Buffer buffer, long amount, TimeUnit unit) throws InterruptedException {
        long target = System.nanoTime() + unit.toNanos(amount);
        while (!pop(buffer)) {
            long wait = Math.min(MAX_WAIT, target - System.nanoTime());
            if (wait <= 0) return false;
            TimeUnit.NANOSECONDS.timedWait(this, wait);
        }
        return true;
    }

    public synchronized void blockingPop(Buffer buffer) throws InterruptedException {
        while (!pop(buffer))
            TimeUnit.NANOSECONDS.timedWait(this, MAX_WAIT);
    }

    public synchronized boolean blockingPush(Buffer buffer, long amount, TimeUnit unit) throws InterruptedException {
        long target = System.nanoTime() + unit.toNanos(amount);
        while (!push(buffer)) {
            long wait = Math.min(MAX_WAIT, target - System.nanoTime());
            if (wait <= 0) return false;
            TimeUnit.NANOSECONDS.timedWait(this, wait);
        }
        return true;
    }

    public synchronized void blockingPush(Buffer buffer) throws InterruptedException {
        while (!push(buffer))
            TimeUnit.NANOSECONDS.timedWait(this, MAX_WAIT);
    }

    public synchronized boolean pop(Buffer buffer) {
        if (popPaused) return false;
        if (!innerPop(buffer))
            return false;
        else {
            notifyAll();
            return true;
        }
    }

    public synchronized boolean push(Buffer buffer) {
        if (pushPaused) return false;
        if (!innerPush(buffer))
            return false;
        else {
            notifyAll();
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

    @Override
    public void close() {
        queue.close();
    }
}
