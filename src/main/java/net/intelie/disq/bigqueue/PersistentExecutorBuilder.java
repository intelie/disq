package net.intelie.disq.bigqueue;

import java.nio.file.Path;

public class PersistentExecutorBuilder {
    private final Path directory;
    private int dataPageSize = BigArrayImpl.DEFAULT_DATA_PAGE_SIZE;

    public PersistentExecutorBuilder(Path directory) {
        this.directory = directory;
    }

    public PersistentExecutorBuilder withPageSize(int size) {
        this.dataPageSize = size;
        return this;
    }


}
