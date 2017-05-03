package net.intelie.disq;

import java.io.IOException;

public interface RawQueue extends AutoCloseable {
    void reopen() throws IOException;

    long bytes();

    long count();

    long remainingBytes();

    long remaningCount();

    void clear() throws IOException;

    boolean pop(Buffer buffer) throws IOException;

    boolean peek(Buffer buffer) throws IOException;

    boolean push(Buffer buffer) throws IOException;

    void flush() throws IOException;

    void close();
}