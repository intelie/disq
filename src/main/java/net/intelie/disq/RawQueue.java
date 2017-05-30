package net.intelie.disq;

import java.io.IOException;

public interface RawQueue extends AutoCloseable {
    void reopen();

    long bytes();

    long count();

    long remainingBytes();

    long remainingCount();

    long nextTimestamp();

    void touch() throws IOException;

    void clear() throws IOException;

    boolean pop(Buffer buffer) throws IOException;

    boolean peek(Buffer buffer) throws IOException;

    boolean push(Buffer buffer) throws IOException;

    void flush() throws IOException;

    void close();
}
