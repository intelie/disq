package net.intelie.disq;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ByteQueue implements Closeable {
    private final Path directory;
    private final IndexFile index;
    private final long dataFileLimit;
    private DataFileReader read;
    private DataFileWriter writer;

    public ByteQueue(Path directory, long dataFileLimit) throws IOException {
        this.directory = directory;
        this.dataFileLimit = dataFileLimit;

        Files.createDirectories(directory);

        this.index = new IndexFile(directory.resolve("index"));
        this.writer = openWriter();
        this.read = openReader();

    }

    private Path makeDataPath(int index) {
        return directory.resolve("data" + index);
    }

    public DataFileReader openReader() throws IOException {
        return new DataFileReader(makeDataPath(index.getReadFile()), index.getReadPosition());
    }

    private DataFileWriter openWriter() throws IOException {
        return new DataFileWriter(makeDataPath(index.getWriteFile()), index.getWritePosition());
    }

    public synchronized long bytes() {
        return index.getBytes();
    }

    public synchronized long count() {
        return index.getCount();
    }

    public synchronized int peekNextSize() throws IOException {
        return read.peekNextSize();
    }

    public synchronized int pop(byte[] buffer, int start) throws IOException {
        if (checkReadEOF())
            return -1;

        int read = this.read.read(buffer, start);
        index.addReadCount(read);
        index.flush();

        checkReadEOF();
        return read;
    }

    public synchronized void push(byte[] buffer, int start, int count) throws IOException {
        checkWriteEOF();
        int written = writer.write(buffer, start, count);
        index.addWriteCount(written);
        index.flush();
    }

    private boolean checkReadEOF() throws IOException {
        while (read.eof() && index.getReadPosition() > 0)
            deleteOldestFile();
        return read.eof();
    }

    public synchronized void deleteOldestFile() throws IOException {
        int currentFile = index.getReadFile();

        if (currentFile == index.getWriteFile())
            advanceWriteFile();

        index.advanceReadFile(read.size());

        read.close();
        read = openReader();

        index.flush();

        Files.delete(makeDataPath(currentFile));
    }

    private void checkWriteEOF() throws IOException {
        if (writer.size() >= dataFileLimit)
            advanceWriteFile();
    }

    private void advanceWriteFile() throws IOException {
        writer.close();
        index.advanceWriteFile();
        writer = openWriter();
    }

    @Override
    public void close() throws IOException {
        read.close();
        writer.close();
        index.close();
    }
}
