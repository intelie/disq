package net.intelie.disq;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class IndexFile {
    public static final int MAX_FILE_ID = 65536;
    public static final int POS_READ_FILE = 0;
    public static final int POS_READ_POSITION = 4;
    public static final int POS_WRITE_FILE = 12;
    public static final int POS_COUNT = 16;
    private final MappedByteBuffer mapped;

    public IndexFile(Path file) throws IOException {
        RandomAccessFile fileObj = new RandomAccessFile(file.toFile(), "rw");
        mapped = fileObj.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 4 * 8);
        fileObj.close();
    }

    public int getReadFile() {
        return mapped.getInt(0);
    }

    public int advanceReadFile() {
        int nextFile = (getReadFile() + 1) % MAX_FILE_ID;
        mapped.putInt(POS_READ_FILE, nextFile);
        setReadPosition(0);
        return nextFile;
    }

    public int advanceWriteFile() {
        int nextFile = (getWriteFile() + 1) % MAX_FILE_ID;
        mapped.putInt(POS_WRITE_FILE, nextFile);
        return nextFile;
    }

    public long getReadPosition() {
        return mapped.getLong(POS_READ_POSITION);
    }

    public void setReadPosition(long value) {
        mapped.putLong(POS_READ_POSITION, value);
    }

    public void advancePosition(int value) {
        setReadPosition(getReadPosition() + value);
    }

    public int getWriteFile() {
        return mapped.getInt(POS_WRITE_FILE);
    }

    public long getCount() {
        return mapped.getLong(POS_COUNT);
    }

    public void addCount(int value) {
        mapped.putLong(POS_COUNT, getCount() + value);
    }

    public void flush() {
        //noflush!
        //mapped.force();
    }
}
