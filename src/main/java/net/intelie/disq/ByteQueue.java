package net.intelie.disq;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ByteQueue implements Closeable {
    private final Path directory;
    private final long dataFileLimit;
    private final Lenient lenient;

    private IndexFile index;
    private DataFileReader reader;
    private DataFileWriter writer;

    public ByteQueue(Path directory, long dataFileLimit) throws IOException {
        this.directory = directory;
        this.dataFileLimit = dataFileLimit;
        this.lenient = new Lenient(this);

        reopen();
    }

    public synchronized void reopen() throws IOException {
        close();
        Files.createDirectories(this.directory);
        this.index = new IndexFile(this.directory.resolve("index"));
        this.writer = openWriter();
        this.reader = null;
        gc();
    }

    public synchronized long bytes() {
        return index.getBytes();
    }

    public synchronized long count() {
        return index.getCount();
    }

    public synchronized int peekNextSize() throws IOException {
        return lenient.perform(() -> reader().peekNextSize());
    }

    public synchronized void clear() throws IOException {
        lenient.perform(() -> {
            index.clear();
            index.flush();
            reopen();
        });
    }

    public synchronized int pop(byte[] buffer, int start) throws IOException {
        return lenient.perform(() -> {
            if (checkReadEOF())
                return -1;

            int read = reader().read(buffer, start);
            index.addReadCount(read);
            index.flush();

            checkReadEOF();
            return read;
        });
    }

    public synchronized boolean deleteOldestFile() throws IOException {
        return lenient.perform(() -> {
            int currentFile = index.getReadFile();

            if (currentFile == index.getWriteFile())
                return false;

            index.advanceReadFile(reader().size());

            reader.close();
            index.flush();
            reader = null;
            tryDeleteFile(currentFile);

            return true;
        });
    }


    public synchronized void push(byte[] buffer, int start, int count) throws IOException {
        lenient.perform(() -> {
            checkWriteEOF();

            int written = writer().write(buffer, start, count);
            index.addWriteCount(written);
            index.flush();

            checkWriteEOF();
        });
    }

    @Override
    public void close() throws IOException {
        lenient.safeClose(reader);
        lenient.safeClose(writer);
        lenient.safeClose(index);
    }

    private boolean checkReadEOF() throws IOException {
        while (index.getReadFile() != index.getWriteFile() && reader().eof())
            deleteOldestFile();
        return index.getCount() == 0;
    }

    private DataFileReader reader() throws IOException {
        return reader != null ? reader : (reader = openReader());
    }

    private void checkWriteEOF() throws IOException {
        if (writer().size() >= dataFileLimit)
            advanceWriteFile();
    }

    private DataFileWriter writer() throws IOException {
        return writer != null ? writer : (writer = openWriter());
    }

    private void advanceWriteFile() throws IOException {
        writer().close();
        index.advanceWriteFile();
        index.flush();
        writer = null;
    }

    private void gc() throws IOException {
        Path file = makeDataPath(index.getReadFile());
        boolean shouldFlush = false;
        while (!Files.exists(file) && index.getReadFile() != index.getWriteFile()) {
            index.advanceReadFile(0);
            file = makeDataPath(index.getReadFile());
            shouldFlush = true;
        }
        long totalBytes = 0;
        long totalCount = 0;
        for (int i = 0; i < IndexFile.MAX_FILES; i++) {
            Path path = makeDataPath(i);
            if (Files.exists(path)) {
                if (!index.isInUse(i)) {
                    tryDeleteFile(i);
                } else {
                    totalBytes += Files.size(path);
                    totalCount += index.getFileCount(i);
                }
            }
        }

        shouldFlush |= index.fixCounts(totalCount, totalBytes);

        if (shouldFlush)
            index.flush();

    }

    private void tryDeleteFile(int file) {
        try {
            Files.delete(makeDataPath(file));
        } catch (Exception ignored) {
            //there is no problem in not being able to delete some files
        }
    }

    private Path makeDataPath(int index) {
        return directory.resolve("data" + String.format("%02x", index));
    }

    private DataFileReader openReader() throws IOException {
        return new DataFileReader(makeDataPath(index.getReadFile()), index.getReadPosition());
    }

    private DataFileWriter openWriter() throws IOException {
        Files.createDirectories(directory);
        return new DataFileWriter(makeDataPath(index.getWriteFile()), index.getWritePosition());
    }

}
