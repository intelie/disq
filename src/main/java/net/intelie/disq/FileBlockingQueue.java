package net.intelie.disq;

import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

class FileBlockingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {
    private final ByteQueue queue;
    private final Serializer<T> serializer;
    private final long maxSize;

    public FileBlockingQueue(Serializer<T> serializer, Path path, long maxSize) throws IOException {
        maxSize = maxSize >= 0 ? maxSize : IndexFile.MAX_QUEUE_SIZE;
        this.serializer = serializer;
        this.queue = new ByteQueue(path, maxSize / IndexFile.MAX_FILES + (maxSize % IndexFile.MAX_FILES > 0 ? 1 : 0));
        this.maxSize = maxSize;

    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException("This queue does not support iteration");
    }

    @Override
    public boolean isEmpty() {
        return queue.count() <= 0;
    }

    @Override
    public int size() {
        return (int) queue.count();
    }

    @Override
    public boolean offer(T obj) {
        return false;
    }

    @Override
    public void put(T t) throws InterruptedException {

    }

    @Override
    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public T take() throws InterruptedException {
        return null;
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public int remainingCapacity() {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        return 0;
    }

    @Override
    public T poll() {
        return null;
    }

    @Override
    public T peek() {
        return null;
    }
}
