package net.intelie.disq;

import java.io.Closeable;
import java.io.IOException;

public class Disq<T> implements Closeable {
    private final PersistentQueue<T> queue;
    private final ThreadPool<T> pool;

    public Disq(PersistentQueue<T> queue, ThreadPool<T> pool) {
        this.queue = queue;
        this.pool = pool;
    }

    public static <T> DisqBuilder<T> builder() {
        return new DisqBuilder<T>(x -> {
        });
    }

    public static <T> DisqBuilder<T> builder(Processor<T> processor) {
        return new DisqBuilder<T>(processor);
    }

    public long count() {
        return queue.count();
    }

    public long bytes() {
        return queue.bytes();
    }

    public long remainingBytes() {
        return queue.remainingBytes();
    }

    public boolean submit(T obj) throws IOException {
        return queue.push(obj);
    }

    public void pause() {
        queue.setPopPaused(true);
    }

    public void resume() {
        queue.setPopPaused(false);
    }

    @Override
    public void close() throws IOException {
        queue.setPushPaused(true);
        pool.close();
    }


}
