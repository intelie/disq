package net.intelie.disq;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class DataFileReader implements Closeable {
    private final DataInputStream stream;

    public DataFileReader(Path file, long position) throws IOException {
        stream = new DataInputStream(new BufferedInputStream(new FileInputStream(file.toFile())));
        stream.skip(position);
    }

    public int peekNextSize() throws IOException {
        stream.mark(8);
        int answer = stream.readInt();
        stream.reset();
        return answer;
    }

    public int read(byte[] bytes, int offset) throws IOException {
        int size = stream.readInt();
        int total = 0;

        while (size > 0) {
            int read = stream.read(bytes, offset, size);
            size -= read;
            offset += read;
            total += read;
        }

        return total;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
