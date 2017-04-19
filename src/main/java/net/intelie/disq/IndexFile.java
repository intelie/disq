package net.intelie.disq;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class IndexFile {
    private final MappedByteBuffer mapped;

    public IndexFile(Path file) throws IOException {
        RandomAccessFile fileObj = new RandomAccessFile(file.toFile(), "rw");
        mapped = fileObj.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 4 * 8);
        fileObj.close();
    }

    public long getReadFile() {
        return mapped.getLong(0);
    }

    public void setReadFile(long value) {
        mapped.putLong(0, value);
    }

    public long getReadPosition() {
        return mapped.getLong(8);
    }

    public void setReadPosition(long value) {
        mapped.putLong(8, value);
    }

    public long getWriteFile() {
        return mapped.getLong(16);
    }

    public void setWriteFile(long value) {
        mapped.putLong(16, value);
    }

    public long getCount() {
        return mapped.getLong(24);
    }

    public void setCount(long value) {
        mapped.putLong(24, value);
    }

    public void flush() {
        mapped.force();
    }
}
