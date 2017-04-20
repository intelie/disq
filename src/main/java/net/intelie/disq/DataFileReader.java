package net.intelie.disq;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class DataFileReader implements Closeable {
    private final DataInputStream stream;
    private final FileInputStream fos;
    private long position;
    private boolean eof = false;

    public DataFileReader(Path file, long position) throws IOException {
        fos = new FileInputStream(file.toFile());
        stream = new DataInputStream(new BufferedInputStream(fos));
        stream.skip(position);
        this.position = position;
    }

    public boolean eof() throws IOException {
        return position >= fos.getChannel().size();
    }

    public int peekNextSize() throws IOException {
        if (eof()) return -1;
        stream.mark(4);
        int answer = stream.readInt();
        stream.reset();
        return answer;
    }

    public int read(byte[] bytes, int offset) throws IOException {
        if (eof()) return -1;
        int size = stream.readInt();
        int total = 4;

        while (size > 0) {
            int read = stream.read(bytes, offset, size);
            size -= read;
            offset += read;
            total += read;
        }
        position += total;

        return total;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
