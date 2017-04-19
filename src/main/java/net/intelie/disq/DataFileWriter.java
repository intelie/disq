package net.intelie.disq;

import java.io.*;
import java.nio.file.Path;

public class DataFileWriter implements Closeable {
    private final DataOutputStream stream;
    private final FileOutputStream fos;

    public DataFileWriter(Path file) throws IOException {
        fos = new FileOutputStream(file.toFile(), true);
        stream = new DataOutputStream(new BufferedOutputStream(fos));
    }

    public long size() throws IOException {
        return fos.getChannel().size();
    }

    public void write(byte[] bytes, int offset, int count) throws IOException {
        stream.writeInt(count);
        stream.write(bytes, offset, count);
        stream.flush();
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
