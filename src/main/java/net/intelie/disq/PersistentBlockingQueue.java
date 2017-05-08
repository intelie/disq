package net.intelie.disq;

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
        queue.blockingPush(obj);
    }

    @Override
    public boolean offer(T obj, long timeout, TimeUnit unit) throws InterruptedException {
        return queue.blockingPush(obj, timeout, unit);
    }

    @Override
    public T take() throws InterruptedException {
        return queue.blockingPop();
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.blockingPop(timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return (int) Math.min(queue.remainingCount(), Integer.MAX_VALUE);
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
        return queue.push(obj);
    }

    @Override
    public T poll() {
        return queue.pop();
    }

    @Override
    public T peek() {
        return queue.peek();
    }
}
