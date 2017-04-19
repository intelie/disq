package net.intelie.disq;

import java.io.IOException;
import java.nio.file.Path;

public class ByteQueue {
    private final Path directory;
    private final IndexFile index;
    private DataFileReader read;
    private DataFileWriter writer;

    public ByteQueue(Path directory, long dataFileSize, long indexFileSize, int maxDataFiles) throws IOException {
        this.directory = directory;
        this.index = new IndexFile(directory.resolve("index"));

        this.read = new DataFileReader(directory.resolve("data" + index.getReadFile()), index.getReadPosition());

    }


    public synchronized long count() {
        return index.getCount();
    }

    public int peekNextSize() {
        return 0;
    }

    public int pop(byte[] buffer, int start) {
        return 0;
    }

    public void push(byte[] buffer, int start, int count) {

    }

}
