package net.intelie.disq;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SerializerPool<T> {
    private final ObjectPool<Slot> pool;
    private final SerializerFactory<T> factory;
    private final int initialBufferSize;
    private final int maxBufferSize;

    public SerializerPool(SerializerFactory<T> factory, int initialBufferSize, int maxBufferSize) {
        this.factory = factory;
        this.initialBufferSize = initialBufferSize;
        this.maxBufferSize = maxBufferSize;
        this.pool = new ObjectPool<>(Slot::new);
    }

    public Slot acquire() {
        return pool.acquire().obj();
    }

    public class Slot implements Closeable {
        private final ObjectPool<Slot>.Ref ref;
        private final Serializer<T> serializer;
        private final Buffer buffer;

        public Slot(ObjectPool<Slot>.Ref ref) {
            this.ref = ref;
            this.serializer = factory.create();
            this.buffer = new Buffer(initialBufferSize, maxBufferSize);
        }


        @Override
        public void close() {
            this.ref.close();
        }

        public boolean push(PersistentQueue queue, T obj) throws IOException {
            serializer.serialize(buffer, obj);
            return queue.push(buffer);
        }

        public T pop(PersistentQueue queue) throws IOException {
            if (!queue.pop(buffer))
                return null;
            return serializer.deserialize(buffer);
        }

        public T peek(PersistentQueue queue) throws IOException {
            if (!queue.peek(buffer))
                return null;
            return serializer.deserialize(buffer);
        }

        public T blockingPop(PersistentQueue queue) throws InterruptedException, IOException {
            queue.blockingPop(buffer);
            return serializer.deserialize(buffer);
        }

        public T blockingPop(PersistentQueue queue, long amount, TimeUnit unit) throws InterruptedException, IOException {
            if (!queue.blockingPop(buffer, amount, unit))
                return null;
            return serializer.deserialize(buffer);
        }

        public void blockingPush(PersistentQueue queue, T obj) throws InterruptedException, IOException {
            serializer.serialize(buffer, obj);
            queue.blockingPush(buffer);
        }

        public boolean blockingPush(PersistentQueue queue, T obj, long amount, TimeUnit unit) throws InterruptedException, IOException {
            serializer.serialize(buffer, obj);
            return queue.blockingPush(buffer, amount, unit);
        }


        public Buffer buffer() {
            return buffer;
        }
    }
}
