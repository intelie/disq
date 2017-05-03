package net.intelie.disq;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class PersistentBlockingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {
    private final ObjectQueue<T> queue;

    public PersistentBlockingQueue(ObjectQueue<T> queue) {
        this.queue = queue;
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException("This queue does not support iteration");
    }

    @Override
    public int size() {
        return (int) Math.min(queue.count(), Integer.MAX_VALUE);
    }

    @Override
    public void put(T obj) throws InterruptedException {

    }

    @Override
    public boolean offer(T obj, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(obj);
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
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        throw new UnsupportedOperationException("This queue does not support draining");
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        throw new UnsupportedOperationException("This queue does not support draining");
    }

    @Override
    public boolean offer(T obj) {
        try {
            return queue.push(obj);
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public T poll() {
        try {
            return queue.pop();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public T peek() {
        try {
            return queue.peek();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
