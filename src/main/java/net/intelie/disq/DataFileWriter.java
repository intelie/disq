package net.intelie.disq;

import java.io.*;
import java.nio.file.Path;

public class DataFileWriter implements Closeable {
    private final DataOutputStream stream;
    private final FileOutputStream fos;
    private final File file;

    public DataFileWriter(Path file) throws IOException {
        this.file = file.toFile();
        fos = new FileOutputStream(this.file, true);
        stream = new DataOutputStream(new BufferedOutputStream(fos));
    }

    public long size() throws IOException {
        return fos.getChannel().size();
    }

    public void write(byte[] bytes, int offset, int count) throws IOException {
        extendFileSizeAtomically(count);
        stream.writeInt(count);
        stream.write(bytes, offset, count);
        stream.flush();
    }

    private void extendFileSizeAtomically(int count) throws IOException {
        RandomAccessFile rand = new RandomAccessFile(file, "rw");
        rand.setLength(rand.getChannel().size() + 4 + count);
        rand.close();
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
