package net.intelie.disq;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ByteQueue implements Closeable {
    private final Path directory;
    private final IndexFile index;
    private final long dataFileLimit;
    private DataFileReader reader;
    private DataFileWriter writer;

    public ByteQueue(Path directory, long dataFileLimit) throws IOException {
        this.directory = directory;
        this.dataFileLimit = dataFileLimit;

        Files.createDirectories(directory);

        this.index = new IndexFile(directory.resolve("index"));
        this.writer = openWriter();
        this.reader = openReader();
    }

    private Path makeDataPath(int index) {
        return directory.resolve("data" + String.format("%02x", index));
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
        return reader.peekNextSize();
    }

    public synchronized void clear() throws IOException {
        index.clear();
        index.flush();
    }

    public synchronized int pop(byte[] buffer, int start) throws IOException {
        if (checkReadEOF())
            return -1;

        int read = ensureReader().read(buffer, start);
        index.addReadCount(read);
        index.flush();

        checkReadEOF();
        return read;
    }

    private boolean checkReadEOF() throws IOException {
        while (index.getReadFile() != index.getWriteFile() && ensureReader().eof())
            deleteOldestFile();
        return index.getCount() == 0;
    }

    private DataFileReader ensureReader() throws IOException {
        return reader != null ? reader : (reader = openReader());
    }

    public synchronized boolean deleteOldestFile() throws IOException {
        int currentFile = index.getReadFile();

        if (currentFile == index.getWriteFile())
            return false;

        index.advanceReadFile(reader.size());

        reader.close();
        index.flush();
        reader = null;

        tryDeleteFile(currentFile);

        return true;
    }


    public synchronized void push(byte[] buffer, int start, int count) throws IOException {
        checkWriteEOF();

        int written = ensureWriter().write(buffer, start, count);
        index.addWriteCount(written);
        index.flush();

        checkWriteEOF();
    }

    private void checkWriteEOF() throws IOException {
        ensureWriter();
        if (writer.size() >= dataFileLimit)
            advanceWriteFile();
    }

    private DataFileWriter ensureWriter() throws IOException {
        return writer != null ? writer : (writer = openWriter());
    }

    private void advanceWriteFile() throws IOException {
        writer.close();
        index.advanceWriteFile();
        index.flush();
        writer = null;
    }

    private void gc(boolean deleteAll) {

    }

    private void tryDeleteFile(int file) {
        try {
            Files.delete(makeDataPath(file));
        } catch (Exception ignored) {
            //there is no problem in not being able to delete some files
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
        writer.close();
        index.close();
    }
}
