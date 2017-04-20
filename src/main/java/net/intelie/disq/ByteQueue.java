package net.intelie.disq;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ByteQueue {
    private final Path directory;
    private final IndexFile index;
    private final long dataFileLimit;
    private final int maxDataFiles;
    private DataFileReader read;
    private DataFileWriter writer;

    public ByteQueue(Path directory, long dataFileLimit, int maxDataFiles) throws IOException {
        this.directory = directory;
        this.dataFileLimit = dataFileLimit;
        this.maxDataFiles = maxDataFiles;

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
        return new DataFileWriter(makeDataPath(index.getWriteFile()));
    }

    public synchronized long count() {
        return index.getCount();
    }

    public synchronized int peekNextSize() throws IOException {
        return read.peekNextSize();
    }

    public synchronized int pop(byte[] buffer, int start) throws IOException {
        if (checkReadEOF()) {
            index.flush();
            return -1;
        }

        int read = this.read.read(buffer, start);
        if (!checkReadEOF())
            index.advancePosition(read);
        index.addCount(-1);
        index.flush();
        return read;
    }

    private boolean checkReadEOF() throws IOException {
        while (read.eof()) {
            if (index.getReadFile() != index.getWriteFile()) {
                read.close();
                int currentFile = index.getReadFile();
                index.advanceReadFile();
                Files.delete(makeDataPath(currentFile));
                read = openReader();
            } else {
                return true;
            }
        }
        return false;
    }

    public synchronized void push(byte[] buffer, int start, int count) throws IOException {
        if (writer.size() >= dataFileLimit) {
            writer.close();
            index.advanceWriteFile();
            writer = openWriter();
        }

        writer.write(buffer, start, count);
        index.addCount(1);
        index.flush();
    }

}
