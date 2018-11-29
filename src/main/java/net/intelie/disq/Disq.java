package net.intelie.disq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Disq<T> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Disq.class);

    private final List<Thread> threads;
    private final long autoFlushNanos;
    private final List<Object> locks;
    private final PersistentQueue<T> queue;
    private final AtomicLong nextFlush;
    private volatile AtomicBoolean open;


    public Disq(ThreadFactory factory, int threads, long autoFlushMs, Processor<T> processor, PersistentQueue<T> queue) {
        this.threads = new ArrayList<>(threads);
        this.autoFlushNanos = autoFlushMs * 1_000_000;
        this.locks = new ArrayList<>();
        this.queue = queue;
        this.open = new AtomicBoolean(true);
        this.nextFlush = autoFlushNanos > 0 ? new AtomicLong(System.nanoTime() + autoFlushNanos) : null;

        for (int i = 0; i < threads; i++) {
            Object shutdownLock = new Object();
            Thread thread = factory.newThread(new WorkerRunnable(queue, shutdownLock, processor));
            this.locks.add(shutdownLock);
            this.threads.add(thread);

            thread.start();
        }
    }

    public static <T> DisqBuilder<T> builder() {
        return new DisqBuilder<T>(null);
    }

    public static <T> DisqBuilder<T> builder(Processor<T> processor) {
        return new DisqBuilder<T>(processor);
    }

    public PersistentQueue<T> queue() {
        return queue;
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
        return open.get() && queue.push(obj);
    }

    public void pause() {
        queue.setPopPaused(true);
    }

    public void resume() {
        queue.setPopPaused(false);
    }

    public void clear() throws IOException {
        queue.clear();
    }

    public void flush() throws IOException {
        queue.flush();
    }

    @Override
    public void close() throws IOException, InterruptedException {
        if (!open.getAndSet(false)) return;
        queue.setPushPaused(true);
        try {
            for (int i = 0; i < threads.size(); i++) {
                synchronized (locks.get(i)) {
                    threads.get(i).interrupt();
                }
            }
            for (Thread thread : threads)
                thread.join();
        } finally {
            queue.close();
        }
    }

    private class WorkerRunnable implements Runnable {
        private final PersistentQueue<T> queue;
        private final Object shutdownLock;
        private final Processor<T> processor;

        public WorkerRunnable(PersistentQueue<T> queue, Object shutdownLock, Processor<T> processor) {
            this.queue = queue;
            this.shutdownLock = shutdownLock;
            this.processor = processor;
        }

        @Override
        public void run() {
            while (open.get()) {
                try {
                    long nextFlushNanos = nextFlush != null ? nextFlush.get() : 0;
                    T obj = blockingPop(nextFlushNanos);

                    process(obj);

                    maybeFlush(nextFlushNanos);
                } catch (Throwable e) {
                    LOGGER.info("Exception processing element", e);
                }
            }
        }

        private void process(T obj) throws Exception {
            if (obj == null || processor == null) return;
            synchronized (shutdownLock) {
                //this lock only exists to avoid a regular interrupt
                //during processor execution
                boolean interrupted = Thread.interrupted();
                try {
                    processor.process(obj);
                } finally {
                    if (interrupted) Thread.currentThread().interrupt();
                }
            }
        }

        private void maybeFlush(long nextFlushNanos) throws IOException {
            long now = System.nanoTime();
            if (nextFlush != null && now >= nextFlushNanos && nextFlush.compareAndSet(nextFlushNanos, now + autoFlushNanos))
                queue.flush();
        }

        private T blockingPop(long nextFlushNanos) throws IOException {
            T obj = null;
            try {
                if (nextFlush != null) {
                    long wait = Math.max(nextFlushNanos - System.nanoTime(), 0);
                    obj = queue.blockingPop(wait, TimeUnit.NANOSECONDS);
                } else {
                    obj = queue.blockingPop();
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            return obj;
        }
    }

}
