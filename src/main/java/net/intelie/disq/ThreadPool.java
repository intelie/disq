package net.intelie.disq;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class ThreadPool<T> implements Closeable {
    private final List<Thread> threads;
    private final Processor<T> processor;
    private final PersistentQueue<T> queue;
    private volatile boolean open = true;

    public ThreadPool(ThreadFactory factory, int threads, Processor<T> processor, PersistentQueue<T> queue) {
        this.threads = new ArrayList<>(threads);
        this.processor = processor;
        this.queue = queue;

        for (int i = 0; i < threads; i++) {
            this.threads.add(factory.newThread(new Runnable() {
                @Override
                public void run() {
                    while (open) {
                        try {
                            T obj = queue.blockingPop();
                            processor.process(obj);
                        } catch (Exception e) {

                        }
                    }
                }
            }));
        }
    }


    @Override
    public void close() throws IOException {
        open = false;
    }
}
