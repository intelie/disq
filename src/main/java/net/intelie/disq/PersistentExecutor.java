package net.intelie.disq;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class PersistentExecutor<T> {
    private final ExecutorService executor;
    private final Processor<T> processor;

    public PersistentExecutor(
            ExecutorService executor,
            Processor<T> processor,
            Serializer<T> serializer) {
        this.executor = executor;
        this.processor = processor;
    }

    public void submit(T task) {

    }
}
