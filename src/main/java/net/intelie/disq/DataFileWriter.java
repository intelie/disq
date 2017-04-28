package net.intelie.disq;

import java.io.*;
import java.nio.file.Path;

public class DataFileWriter implements Closeable {
    public static final int OVERHEAD = 4;
    private final DataOutputStream stream;
    private final FileOutputStream fos;
    private final File file;

    public DataFileWriter(Path file, long position) throws IOException {
        this.file = file.toFile();
        setLength(this.file, position);
        fos = new FileOutputStream(this.file, true);
        stream = new DataOutputStream(new BufferedOutputStream(fos));
    }

    private void setLength(File file, long size) throws IOException {
        RandomAccessFile rand = new RandomAccessFile(file, "rws");
        rand.setLength(size);
        rand.close();
    }

    public long size() throws IOException {
        return fos.getChannel().size();
    }

    public int write(Buffer buffer) throws IOException {
        stream.writeInt(buffer.count());
        stream.write(buffer.buf(), 0, buffer.count());
        stream.flush();
        return buffer.count() + OVERHEAD;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
