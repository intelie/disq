package net.intelie.disq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class PersistentQueue<T> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistentQueue.class);

    private static final long MAX_WAIT = 10_000_000_000L;

    private final ConcurrentLinkedQueue<WeakReference<Buffer>> pool;
    private final ArrayRawQueue fallback;
    private final RawQueue queue;
    private final Serializer<T> serializer;
    private final int initialBufferCapacity;
    private final int maxBufferCapacity;
    private final boolean compress;
    private final Lock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

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
        return this.<T, InterruptedException>doWithBuffer(buffer -> {
            lock.lockInterruptibly();
            try {
                long target = System.nanoTime() + unit.toNanos(amount);
                while (!notifyingPop(buffer)) {
                    long wait = Math.min(MAX_WAIT, target - System.nanoTime());
                    if (wait <= 0) return null;
                    notEmpty.awaitNanos(wait);
                }
            } finally {
                lock.unlock();
            }
            return deserialize(buffer);
        });
    }

    public T blockingPop() throws InterruptedException, IOException {
        return this.<T, InterruptedException>doWithBuffer(buffer -> {
            lock.lockInterruptibly();
            try {
                while (!notifyingPop(buffer))
                    notEmpty.awaitNanos(MAX_WAIT);
            } finally {
                lock.unlock();
            }
            return deserialize(buffer);
        });
    }

    public boolean blockingPush(T obj, long amount, TimeUnit unit) throws InterruptedException, IOException {
        return this.<Boolean, InterruptedException>doWithBuffer(buffer -> {
            serialize(obj, buffer);
            lock.lockInterruptibly();
            try {
                long target = System.nanoTime() + unit.toNanos(amount);
                while (!notifyingPush(buffer)) {
                    long wait = Math.min(MAX_WAIT, target - System.nanoTime());
                    if (wait <= 0) return false;

                    notFull.awaitNanos(target - System.nanoTime());
                }
            } finally {
                lock.unlock();
            }
            return true;
        });
    }

    public boolean blockingPush(T obj) throws InterruptedException, IOException {
        return this.<Boolean, InterruptedException>doWithBuffer(buffer -> {
            serialize(obj, buffer);
            lock.lockInterruptibly();
            try {
                while (!notifyingPush(buffer))
                    notFull.awaitNanos(MAX_WAIT);
            } finally {
                lock.unlock();
            }
            return true;
        });
    }

    public T pop() throws IOException {
        return doWithBuffer(buffer -> {
            lock.lock();
            try {
                if (!notifyingPop(buffer)) return null;
            } finally {
                lock.unlock();
            }
            return deserialize(buffer);
        });
    }

    public boolean push(T obj) throws IOException {
        return doWithBuffer(buffer -> {
            serialize(obj, buffer);
            lock.lock();
            try {
                if (!notifyingPush(buffer)) return false;
            } finally {
                lock.unlock();
            }
            return true;
        });
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

    private T deserialize(Buffer buffer) throws IOException {
        InputStream read = buffer.read();
        if (compress) read = new InflaterInputStream(read);
        return (T) serializer.deserialize(read);
    }

    private void serialize(T obj, Buffer buffer) throws IOException {
        if (obj == null)
            throw new NullPointerException("Null elements not allowed in persistent queue.");

        OutputStream write = buffer.write();
        if (compress) write = new DeflaterOutputStream(write);
        try {
            serializer.serialize(write, obj);
        } finally {
            write.close();
        }
    }

    public T peek() throws IOException {
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

    private <Q, E extends Throwable> Q doWithBuffer(BufferOp<Q, E> op) throws IOException, E {
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
            buffer.reset();
            return op.call(buffer);
        } finally {
            pool.offer(ref);
        }
    }

    public void close() {
        queue.close();
    }

    private interface BufferOp<Q, E extends Throwable> {
        Q call(Buffer buffer) throws E, IOException;
    }
}
