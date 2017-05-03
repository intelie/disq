package net.intelie.disq;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ArrayRawQueue implements RawQueue {
    private final byte[] memory;

    public ArrayRawQueue(int maxSize, boolean deleteOldestOnOverflow) {
        this.memory = new byte[maxSize];
    }

    @Override
    public void reopen() throws IOException {

    }

    @Override
    public long bytes() {
        return 0;
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public long remainingBytes() {
        return 0;
    }

    @Override
    public long remaningCount() {
        return 0;
    }

    @Override
    public void clear() throws IOException {

    }

    @Override
    public boolean pop(Buffer buffer) throws IOException {
        return false;
    }

    @Override
    public boolean peek(Buffer buffer) throws IOException {
        return false;
    }

    @Override
    public boolean push(Buffer buffer) throws IOException {
        return false;
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void close() {

    }
}
