package net.intelie.disq;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class ObjectQueue<T> implements AutoCloseable {
    private final ConcurrentLinkedQueue<WeakReference<Buffer>> pool;
    private final ByteQueue queue;
    private final Serializer<T> serializer;

    public ObjectQueue(ByteQueue queue, Serializer<T> serializer) {
        this.pool = new ConcurrentLinkedQueue<>();
        this.queue = queue;
        this.serializer = serializer;
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
                while (queue.pop(buffer) < 0)
                    queue.wait(target - System.currentTimeMillis());
            }
            return serializer.deserialize(buffer.read());
        });
    }

    public T blockingPop() throws IOException, InterruptedException {
        return doWithBuffer(buffer -> {
            synchronized (queue) {
                while (queue.pop(buffer) < 0)
                    queue.wait();
            }
            return serializer.deserialize(buffer.read());
        });
    }

    public T pop() throws IOException {
        return doWithBuffer(buffer -> {
            if (queue.pop(buffer) < 0)
                return null;
            return serializer.deserialize(buffer.read());
        });
    }

    public boolean push(T obj) throws IOException {
        return doWithBuffer(buffer -> {
            serializer.serialize(buffer.write(), obj);
            synchronized (queue) {
                queue.push(buffer);

            }
        });
    }

    public T peek() throws IOException {
        return doWithBuffer(buffer -> {
            if (queue.peek(buffer) < 0)
                return null;
            return serializer.deserialize(buffer.read());
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
            buffer = new Buffer();
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
