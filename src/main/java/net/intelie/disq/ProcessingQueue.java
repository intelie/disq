package net.intelie.disq;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ProcessingQueue<T> {
    private final Consumer<T> consumer;
    private final ObjectQueue<?> queue;
    private final ThreadPoolExecutor executor;

    public ProcessingQueue(Consumer<T> consumer, ObjectQueue<?> queue, ThreadPoolExecutor executor) {
        this.consumer = consumer;
        this.queue = queue;
        this.executor = executor;
    }

    public static <T> Builder<T> builder(Consumer<T> consumer) {
        return new Builder<T>(consumer);
    }

    public void submit(T obj) {
        executor.submit(new MyRunnable<>(obj, consumer));
    }

    public static class Builder<T> {
        private final Consumer<T> consumer;

        private Serializer<T> serializer = new DefaultSerializer<T>();
        private Path directory = null; //default to temp directory
        private long maxSize = Long.MAX_VALUE;
        private boolean flushOnPop = true;
        private boolean flushOnPush = true;

        private boolean deleteOldestOnOverflow = false;
        private int initialBufferCapacity = 4096;
        private int maxBufferCapacity = -1;
        private boolean compress = false;
        private int fallbackBufferCapacity = 0;
        private int threads = 1;
        private long threadKeepAlive = 60000;
        private ThreadFactory threadFactory = Executors.defaultThreadFactory();

        public Builder(Consumer<T> consumer) {
            this.consumer = consumer;
        }

        public ProcessingQueue<T> build() {
            DiskRawQueue rawQueue = new DiskRawQueue(
                    directory, maxSize, flushOnPop, flushOnPush, deleteOldestOnOverflow);

            ObjectQueue<MyRunnable<T>> objectQueue = new ObjectQueue<>(rawQueue,
                    new MySerializer<>(serializer, consumer), initialBufferCapacity, maxBufferCapacity, compress, fallbackBufferCapacity);

            PersistentBlockingQueue<Runnable> workQueue = new PersistentBlockingQueue(objectQueue);
            return new ProcessingQueue<T>(consumer, objectQueue,
                    new ThreadPoolExecutor(1, threads, threadKeepAlive, TimeUnit.MILLISECONDS, workQueue, threadFactory, new ThreadPoolExecutor.DiscardPolicy()));
        }

    }

    private static class MyRunnable<T> implements Runnable {
        private final T obj;
        private final Consumer<T> consumer;

        public MyRunnable(T obj, Consumer<T> consumer) {
            this.obj = obj;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            consumer.accept(obj);
        }
    }

    private static class MySerializer<T> implements Serializer<MyRunnable<T>> {
        private final Serializer<T> serializer;
        private final Consumer<T> consumer;

        private MySerializer(Serializer<T> serializer, Consumer<T> consumer) {
            this.serializer = serializer;
            this.consumer = consumer;
        }


        @Override
        public void serialize(OutputStream stream, MyRunnable<T> obj) throws IOException {
            serializer.serialize(stream, obj.obj);
        }

        @Override
        public MyRunnable<T> deserialize(InputStream stream) throws IOException {
            return new MyRunnable<T>(serializer.deserialize(stream), consumer);
        }
    }
}
