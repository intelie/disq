package net.intelie.disq;

import java.io.*;
import java.nio.file.Path;

public class DataFileWriter implements Closeable {
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
        RandomAccessFile rand = new RandomAccessFile(file, "rw");
        rand.setLength(size);
        rand.close();
    }

    public long size() throws IOException {
        return fos.getChannel().size();
    }

    public int write(byte[] bytes, int offset, int count) throws IOException {
        stream.writeInt(count);
        stream.write(bytes, offset, count);
        stream.flush();
        return count + 4;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
