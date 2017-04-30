package net.intelie.disq;

import java.io.*;
import java.nio.file.Path;

public class DataFileReader implements Closeable {
    private final DataInputStream stream;
    private final FileInputStream fis;
    private long position;

    public DataFileReader(Path file, long position) throws IOException {
        fis = new FileInputStream(file.toFile());
        skipToPosition(position);
        stream = new DataInputStream(new BufferedInputStream(fis));
        this.position = position;
    }

    private void skipToPosition(long position) throws IOException {
        while(position > 0)
            position -= fis.skip(position);
    }

    public boolean eof() throws IOException {
        return position >= size();
    }

    public long size() throws IOException {
        return fis.getChannel().size();
    }

    public int read(Buffer buffer) throws IOException {
        if (eof()) return -1;
        int size = stream.readInt();
        int total = DataFileWriter.OVERHEAD;

        buffer.setCount(size, false);

        int offset = 0;

        while (size > 0) {
            int read = stream.read(buffer.buf(), offset, size);
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