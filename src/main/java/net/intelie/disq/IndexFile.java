package net.intelie.disq;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Random;

public class IndexFile implements Closeable {
    public static final int MAX_FILES = 118;
    public static final int EXPECTED_SIZE = 2 * 4 + 4 * 8 + MAX_FILES * 4;
    public static final long MAX_QUEUE_SIZE = MAX_FILES * (long) Integer.MAX_VALUE;

    private final RandomAccessFile randomWrite;
    private final ByteBuffer buffer = ByteBuffer.allocate(EXPECTED_SIZE);
    private int readFile, writeFile;
    private long readPosition, writePosition;
    private long count;
    private long bytes;
    private int[] fileCounts;

    public IndexFile(Path file) throws IOException {
        this.fileCounts = new int[MAX_FILES];
        this.randomWrite = new RandomAccessFile(file.toFile(), "rw");
        if (Files.exists(file) && Files.size(file) == EXPECTED_SIZE) {
            try (DataInputStream stream = new DataInputStream(new FileInputStream(file.toFile()))) {
                readFile = stream.readInt();
                writeFile = stream.readInt();
                readPosition = stream.readLong();
                writePosition = stream.readLong();
                count = stream.readLong();
                bytes = stream.readLong();
                for (int i = 0; i < MAX_FILES; i++)
                    fileCounts[i] = stream.readInt();
            }
        }
    }

    public boolean isInUse(int file) {
        return readFile <= writeFile && readFile <= file && file <= writeFile ||
                readFile > writeFile && (readFile <= file || file <= writeFile);
    }

    public void flush() throws IOException {
        buffer.position(0);
        buffer.putInt(readFile);
        buffer.putInt(writeFile);
        buffer.putLong(readPosition);
        buffer.putLong(writePosition);
        buffer.putLong(count);
        buffer.putLong(bytes);
        for (int i = 0; i < MAX_FILES; i++)
            buffer.putInt(fileCounts[i]);

        randomWrite.seek(0);
        randomWrite.write(buffer.array());
    }

    public int getReadFile() {
        return readFile;
    }

    public int advanceReadFile(long oldBytes) {
        int oldCount = fileCounts[readFile];
        fileCounts[readFile] = 0;
        count -= oldCount;
        bytes -= oldBytes;
        readFile++;
        readFile %= MAX_FILES;
        readPosition = 0;
        return readFile;
    }

    public int advanceWriteFile() {
        writeFile++;
        writeFile %= MAX_FILES;
        writePosition = 0;
        return writeFile;
    }

    public long getReadPosition() {
        return readPosition;
    }

    public int getWriteFile() {
        return writeFile;
    }

    public long getCount() {
        return count;
    }

    public long getBytes() {
        return bytes;
    }

    public void addWriteCount(int bytes) {
        this.count += 1;
        this.bytes += bytes;
        this.writePosition += bytes;
        this.fileCounts[writeFile] += 1;
    }

    public void addReadCount(int bytes) {
        this.count -= 1;
        //does not decrement this.bytes, only when the file is deleted
        this.readPosition += bytes;
        this.fileCounts[readFile] -= 1;
    }

    public void clear() {
        readPosition = writePosition = count = bytes = 0;
        readFile = writeFile = 0;
        for (int i = 0; i < MAX_FILES; i++)
            fileCounts[i] = 0;
    }

    public long getWritePosition() {
        return writePosition;
    }

    @Override
    public void close() throws IOException {
        randomWrite.close();
    }

    public int getFileCount(int file) {
        return fileCounts[file];
    }

    public boolean fixCounts(long totalCount, long totalBytes) {
        if (totalBytes != bytes || totalCount != count) {
            bytes = totalBytes;
            count = totalCount;
            return true;
        }
        return false;
    }
}
