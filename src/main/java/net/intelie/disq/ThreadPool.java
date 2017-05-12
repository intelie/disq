package net.intelie.disq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class ThreadPool<T> implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadPool.class);

    private final List<Thread> threads;
    private final List<Object> locks;
    private final Processor<T> processor;
    private final PersistentQueue<T> queue;
    private volatile boolean open = true;

    public ThreadPool(ThreadFactory factory, int threads, Processor<T> processor, PersistentQueue<T> queue) {
        this.threads = new ArrayList<>(threads);
        this.locks = new ArrayList<>();
        this.processor = processor;
        this.queue = queue;

        for (int i = 0; i < threads; i++) {
            Object shutdownLock = new Object();
            Thread thread = factory.newThread(() -> {
                while (open) {
                    try {
                        T obj = null;
                        try {
                            obj = queue.blockingPop();
                        } catch (InterruptedException ignored) {
                            continue;
                        }
                        synchronized (shutdownLock) {
                            boolean interrupted = Thread.interrupted();
                            try {
                                processor.process(obj);
                            } finally {
                                if (interrupted) Thread.currentThread().interrupt();
                            }
                        }
                    } catch (Throwable e) {
                        LOGGER.info("Exception processing element", e);
                    }
                }
            });
            this.locks.add(shutdownLock);
            this.threads.add(thread);

            thread.start();
        }
    }

    @Override
    public void close() throws IOException {
        open = false;
        for (int i = 0; i < threads.size(); i++) {
            synchronized (locks.get(i)) {
                threads.get(i).interrupt();
            }
        }
    }
}
