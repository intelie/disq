package net.intelie.disq;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

public class ObjectPool<T> {
    private final ConcurrentLinkedQueue<Ref> queue = new ConcurrentLinkedQueue<>();
    private final Supplier<T> factory;
    private final int maxRetries;

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private final List<T> strong;
    //keeping a strong reference to minPoolSize elements to avoid their collection

    public ObjectPool(Supplier<T> factory) {
        this(factory, 1, 5);
    }

    public ObjectPool(Supplier<T> factory, int minPoolSize, int maxRetries) {
        this.maxRetries = maxRetries;
        this.factory = factory;
        this.strong = initMinPool(factory, minPoolSize);
    }

    private List<T> initMinPool(Supplier<T> factory, int minPoolSize) {
        List<T> strong = new ArrayList<>();
        for (int i = 0; i < minPoolSize; i++) {
            try (Ref ref = new Ref(factory.get())) {
                strong.add(ref.obj());
            }
        }
        return strong;
    }

    public Ref acquire() {
        Ref ref = null;

        for (int i = 0; i < maxRetries; i++) {
            ref = queue.poll();

            //either empty pool or valid deref'd object
            if (ref == null || ref.materialize()) break;

            ref = null;
        }

        if (ref == null)
            ref = new Ref(factory.get());

        return ref;
    }

    public class Ref implements AutoCloseable {
        private final WeakReference<T> ref;
        private T obj;

        private Ref(T obj) {
            this.ref = new WeakReference<>(obj);
            this.obj = obj;
        }

        private boolean materialize() {
            this.obj = ref.get();
            return this.obj != null;
        }

        public T obj() {
            return obj;
        }

        @Override
        public void close() {
            if (obj == null) return;
            obj = null;
            queue.offer(this);
        }
    }
}
