package net.intelie.disq;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

public class StateFile implements Closeable {
    public static final short MAX_FILES = 121;
    public static final short MAX_FILE_ID = Short.MAX_VALUE / MAX_FILES * MAX_FILES;
    //we use this to allow identifying when read = write because they are in fact same file
    //or we just have a full queue

    public static final int EXPECTED_SIZE = 2 * 2 + 2 * 4 + 2 * 8 + MAX_FILES * 4;
    public static final long MIN_QUEUE_SIZE = MAX_FILES * 512;
    public static final long MAX_QUEUE_SIZE = MAX_FILES * (long) Integer.MAX_VALUE;

    private final RandomAccessFile randomWrite;
    private final ByteBuffer buffer = ByteBuffer.allocate(EXPECTED_SIZE);
    private int readFile, writeFile;
    private int readPosition, writePosition;
    private long count;
    private long bytes;
    private int[] fileCounts;

    public StateFile(Path file) throws IOException {
        this.fileCounts = new int[MAX_FILES];
        this.randomWrite = new RandomAccessFile(file.toFile(), "rw");
        if (Files.exists(file) && Files.size(file) == EXPECTED_SIZE) {
            try (DataInputStream stream = new DataInputStream(new FileInputStream(file.toFile()))) {
                readFile = stream.readShort();
                writeFile = stream.readShort();
                readPosition = stream.readInt();
                writePosition = stream.readInt();
                count = stream.readLong();
                bytes = stream.readLong();
                for (int i = 0; i < MAX_FILES; i++)
                    fileCounts[i] = stream.readInt();
            }
        }
    }

    public boolean isInUse(int file) {
        int readFile = getReadFile();
        int writeFile = getWriteFile();
        boolean same = sameFileReadWrite();

        return same && readFile == file ||
                !same && readFile <= writeFile && readFile <= file && file <= writeFile ||
                !same && readFile >= writeFile && (readFile <= file || file <= writeFile);
    }

    public void flush() throws IOException {
        buffer.position(0);
        buffer.putShort((short) readFile);
        buffer.putShort((short) writeFile);
        buffer.putInt(readPosition);
        buffer.putInt(writePosition);
        buffer.putLong(count);
        buffer.putLong(bytes);
        for (int i = 0; i < MAX_FILES; i++)
            buffer.putInt(fileCounts[i]);

        randomWrite.seek(0);
        randomWrite.write(buffer.array());
    }

    public int getReadFile() {
        return readFile % MAX_FILES;
    }

    public boolean sameFileReadWrite() {
        return readFile == writeFile;
    }

    public int advanceReadFile(long oldBytes) {
        int oldCount = fileCounts[getReadFile()];
        fileCounts[getReadFile()] = 0;
        count -= oldCount;
        bytes -= oldBytes;
        readFile++;
        readFile %= MAX_FILE_ID;
        readPosition = 0;
        return readFile;
    }

    public int advanceWriteFile() {
        writeFile++;
        writeFile %= MAX_FILE_ID;
        writePosition = 0;
        return writeFile;
    }

    public long getReadPosition() {
        return readPosition;
    }

    public int getWriteFile() {
        return writeFile % MAX_FILES;
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
        this.fileCounts[getWriteFile()] += 1;
    }

    public void addReadCount(int bytes) {
        this.count -= 1;
        //does not decrement this.bytes, only when the file is deleted
        this.readPosition += bytes;
        this.fileCounts[getReadFile()] -= 1;
    }

    public void clear() {
        readFile = writeFile = 0;
        readPosition = writePosition = 0;
        count = bytes = 0;
        for (int i = 0; i < MAX_FILES; i++)
            fileCounts[i] = 0;
    }

    public long getWritePosition() {
        return writePosition;
    }

    @Override
    public void close() throws IOException {
        flush();
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

    public boolean readFileEof() {
        return fileCounts[getReadFile()] <= 0;
    }
}
