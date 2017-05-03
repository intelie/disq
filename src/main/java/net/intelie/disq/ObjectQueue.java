package net.intelie.disq;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class ObjectQueue<T> implements AutoCloseable {
    private final ConcurrentLinkedQueue<WeakReference<Buffer>> pool;
    private final DiskRawQueue queue;
    private final Serializer<T> serializer;
    private final int initialBufferCapacity;
    private final int maxBufferCapacity;
    private final boolean compress;

    public ObjectQueue(DiskRawQueue queue, Serializer<T> serializer, int initialBufferCapacity, int maxBufferCapacity, boolean compress) {
        this.queue = queue;
        this.serializer = serializer;
        this.initialBufferCapacity = initialBufferCapacity;
        this.maxBufferCapacity = maxBufferCapacity;
        this.compress = compress;
        this.pool = new ConcurrentLinkedQueue<>();
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

    public void clear() throws IOException {
        queue.clear();
    }

    public void flush() throws IOException {
        queue.flush();
    }

    public T blockingPop(long amount, TimeUnit unit) throws IOException, InterruptedException {
        return doWithBuffer(buffer -> {
            synchronized (queue) {
                long target = System.currentTimeMillis() + unit.toMillis(amount);
                while (!queue.pop(buffer)) {
                    long wait = target - System.currentTimeMillis();
                    if (wait <= 0) return null;
                    queue.wait(wait);
                }
            }
            return deserialize(buffer);
        });
    }

    public T blockingPop() throws IOException, InterruptedException {
        return doWithBuffer(buffer -> {
            synchronized (queue) {
                while (!queue.pop(buffer))
                    queue.wait();
            }
            return deserialize(buffer);
        });
    }

    public boolean blockingPush(T obj, long amount, TimeUnit unit) throws IOException, InterruptedException {
        return doWithBuffer(buffer -> {
            serialize(obj, buffer);
            synchronized (queue) {
                long target = System.currentTimeMillis() + unit.toMillis(amount);
                while (!queue.push(buffer)) {
                    long wait = target - System.currentTimeMillis();
                    if (wait <= 0) return false;
                    queue.wait(target - System.currentTimeMillis());
                }
            }
            return true;
        });
    }

    public boolean blockingPush(T obj) throws IOException, InterruptedException {
        return doWithBuffer(buffer -> {
            serialize(obj, buffer);
            synchronized (queue) {
                while (!queue.push(buffer))
                    queue.wait();
            }
            return true;
        });
    }

    public T pop() throws IOException {
        return doWithBuffer(buffer -> {
            synchronized (queue) {
                if (!queue.pop(buffer))
                    return null;
                else
                    queue.notifyAll();
            }
            return deserialize(buffer);
        });
    }

    public boolean push(T obj) throws IOException {
        return doWithBuffer(buffer -> {
            serialize(obj, buffer);
            synchronized (queue) {
                queue.notifyAll();
                return queue.push(buffer);
            }
        });
    }

    private T deserialize(Buffer buffer) throws IOException {
        InputStream read = buffer.read();
        if (compress) read = new InflaterInputStream(read);
        return serializer.deserialize(read);
    }

    private void serialize(T obj, Buffer buffer) throws IOException {
        OutputStream write = buffer.write();
        if (compress) write = new DeflaterOutputStream(write);
        serializer.serialize(write, obj);
    }

    public T peek() throws IOException {
        return doWithBuffer(buffer -> {
            if (!queue.peek(buffer))
                return null;
            return deserialize(buffer);
        });
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
            return op.call(buffer);
        } finally {
            pool.offer(ref);
        }
    }

    public void close() {
        queue.close();
    }

    private interface BufferOp<Q, E extends Throwable> {
        Q call(Buffer buffer) throws IOException, E;
    }
}
