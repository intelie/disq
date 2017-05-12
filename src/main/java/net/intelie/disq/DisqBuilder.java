package net.intelie.disq;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class DisqBuilder<T> {
    private final Processor<T> processor;

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
    private int threadCount = 1;
    private ThreadFactory threadFactory = Executors.defaultThreadFactory();

    public DisqBuilder(Processor<T> processor) {
        this.processor = processor;
    }

    public DisqBuilder<T> setSerializer(Serializer<T> serializer) {
        this.serializer = serializer;
        return this;
    }

    public DisqBuilder<T> setDirectory(Path directory) {
        this.directory = directory;
        return this;
    }

    public DisqBuilder<T> setDirectory(String first, String... rest) {
        return setDirectory(Paths.get(first, rest));
    }

    public DisqBuilder<T> setMaxSize(long maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    public DisqBuilder<T> setFlushOnPop(boolean flushOnPop) {
        this.flushOnPop = flushOnPop;
        return this;
    }

    public DisqBuilder<T> setFlushOnPush(boolean flushOnPush) {
        this.flushOnPush = flushOnPush;
        return this;
    }

    public DisqBuilder<T> setDeleteOldestOnOverflow(boolean deleteOldestOnOverflow) {
        this.deleteOldestOnOverflow = deleteOldestOnOverflow;
        return this;
    }

    public DisqBuilder<T> setInitialBufferCapacity(int initialBufferCapacity) {
        this.initialBufferCapacity = initialBufferCapacity;
        return this;
    }

    public DisqBuilder<T> setMaxBufferCapacity(int maxBufferCapacity) {
        this.maxBufferCapacity = maxBufferCapacity;
        return this;
    }

    public DisqBuilder<T> setCompress(boolean compress) {
        this.compress = compress;
        return this;
    }

    public DisqBuilder<T> setFallbackBufferCapacity(int fallbackBufferCapacity) {
        this.fallbackBufferCapacity = fallbackBufferCapacity;
        return this;
    }

    public DisqBuilder<T> setThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

    public DisqBuilder<T> setNamedThreadFactory(String nameFormat) {
        return setThreadFactory(new NamedThreadFactory(nameFormat));
    }

    public DisqBuilder<T> setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        return this;
    }

    public Disq<T> build() {
        return build(true);
    }

    public Disq<T> build(boolean paused) {
        DiskRawQueue rawQueue = buildRawQueue();

        PersistentQueue<T> persistentQueue = buildPersistentQueue(rawQueue);
        persistentQueue.setPopPaused(paused);

        ThreadPool<T> pool = new ThreadPool<>(threadFactory, threadCount, processor, persistentQueue);
        return new Disq<T>(persistentQueue, pool);
    }

    private PersistentQueue<T> buildPersistentQueue(DiskRawQueue rawQueue) {
        return new PersistentQueue<>(
                rawQueue, serializer, initialBufferCapacity, maxBufferCapacity, compress, fallbackBufferCapacity);
    }

    private DiskRawQueue buildRawQueue() {
        return new DiskRawQueue(
                directory, maxSize, flushOnPop, flushOnPush, deleteOldestOnOverflow);
    }

}
