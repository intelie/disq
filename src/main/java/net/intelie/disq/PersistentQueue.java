package net.intelie.disq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class PersistentQueue<T> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistentQueue.class);

    private static final int MAX_WAIT = 10000;

    private final ConcurrentLinkedQueue<WeakReference<Buffer>> pool;
    private final ArrayRawQueue fallback;
    private final RawQueue queue;
    private final Serializer<T> serializer;
    private final int initialBufferCapacity;
    private final int maxBufferCapacity;
    private final boolean compress;
    private boolean popPaused, pushPaused;

    public PersistentQueue(RawQueue queue, Serializer<T> serializer, int initialBufferCapacity, int maxBufferCapacity, boolean compress) {
        this(queue, serializer, initialBufferCapacity, maxBufferCapacity, compress, 0);
    }

    public PersistentQueue(RawQueue queue, Serializer<T> serializer, int initialBufferCapacity, int maxBufferCapacity, boolean compress, int fallbackBufferCapacity) {
        this.fallback = new ArrayRawQueue(fallbackBufferCapacity, true);
        this.queue = queue;
        this.serializer = serializer;
        this.initialBufferCapacity = initialBufferCapacity;
        this.maxBufferCapacity = maxBufferCapacity;
        this.compress = compress;
        this.pool = new ConcurrentLinkedQueue<>();
    }

    public void setPopPaused(boolean popPaused) {
        synchronized (queue) {
            this.popPaused = popPaused;
            this.queue.notifyAll();
        }
    }

    public void setPushPaused(boolean pushPaused) {
        synchronized (queue) {
            this.pushPaused = pushPaused;
            this.queue.notifyAll();
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

    public T blockingPop(long amount, TimeUnit unit) throws InterruptedException {
        return doWithBuffer(buffer -> {
            synchronized (queue) {
                long target = System.currentTimeMillis() + unit.toMillis(amount);
                while (!notifyingPop(buffer)) {
                    long wait = Math.min(MAX_WAIT, target - System.currentTimeMillis());
                    if (wait <= 0) return null;
                    queue.wait(wait);
                }
            }
            return deserialize(buffer);
        });
    }

    public T blockingPop() throws InterruptedException {
        return doWithBuffer(buffer -> {
            synchronized (queue) {
                while (!notifyingPop(buffer))
                    queue.wait(MAX_WAIT);
            }
            return deserialize(buffer);
        });
    }

    public boolean blockingPush(T obj, long amount, TimeUnit unit) throws InterruptedException {
        return doWithBuffer(buffer -> {
            serialize(obj, buffer);
            synchronized (queue) {
                long target = System.currentTimeMillis() + unit.toMillis(amount);
                while (!notifyingPush(buffer)) {
                    long wait = Math.min(MAX_WAIT, target - System.currentTimeMillis());
                    if (wait <= 0) return false;
                    queue.wait(target - System.currentTimeMillis());
                }
            }
            return true;
        });
    }

    public boolean blockingPush(T obj) throws InterruptedException {
        return doWithBuffer(buffer -> {
            serialize(obj, buffer);
            synchronized (queue) {
                while (!notifyingPush(buffer))
                    queue.wait(MAX_WAIT);
            }
            return true;
        });
    }

    public T pop() {
        return doWithBuffer(buffer -> {
            synchronized (queue) {
                if (!notifyingPop(buffer)) return null;
            }
            return deserialize(buffer);
        });
    }

    public boolean push(T obj) {
        return doWithBuffer(buffer -> {
            serialize(obj, buffer);
            synchronized (queue) {
                if (!notifyingPush(buffer)) return false;
            }
            return true;
        });
    }

    private boolean notifyingPop(Buffer buffer) {
        if (popPaused) return false;
        if (!innerPop(buffer))
            return false;
        else {
            queue.notifyAll();
            return true;
        }
    }

    private boolean notifyingPush(Buffer buffer) {
        if (pushPaused) return false;
        if (!innerPush(buffer))
            return false;
        else {
            queue.notifyAll();
            return true;
        }
    }

    private T deserialize(Buffer buffer) {
        try {
            InputStream read = buffer.read();
            if (compress) read = new InflaterInputStream(read);
            return (T) serializer.deserialize(read);
        } catch (Exception e) {
            LOGGER.info("Error deserializing", e);
            return null;
        }
    }

    private void serialize(T obj, Buffer buffer) {
        try {
            buffer.setCount(0, false);
            OutputStream write = buffer.write();
            if (compress) write = new DeflaterOutputStream(write);
            serializer.serialize(write, obj);
        } catch (Exception e) {
            LOGGER.info("Error serializing", e);
        }
    }

    public T peek() {
        return doWithBuffer(buffer -> {
            if (!innerPeek(buffer))
                return null;
            return deserialize(buffer);
        });
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

    private <Q, E extends Throwable> Q doWithBuffer(BufferOp<Q, E> op) throws E {
        Buffer buffer = null;
        WeakReference<Buffer> ref = null;

        for (int i = 0; i < 5; i++) {
            ref = pool.poll();

            if (ref == null) break; //empty pool

            buffer = ref.get();

            if (buffer != null) break; //valid deref'd object
        }

        if (buffer == null) {
            buffer = new Buffer(initialBufferCapacity, maxBufferCapacity);
            ref = new WeakReference<>(buffer);
        }

        try {
            return op.call(buffer);
        } finally {
            pool.offer(ref);
        }
    }

    public void close() {
        queue.close();
    }

    private interface BufferOp<Q, E extends Throwable> {
        Q call(Buffer buffer) throws E;
    }
}
