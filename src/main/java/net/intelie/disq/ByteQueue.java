package net.intelie.disq;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ByteQueue implements AutoCloseable {
    private final Path directory;
    private final long maxSize;
    private final long dataFileLimit;
    private final Lenient lenient;

    private final boolean flushOnRead;
    private final boolean flushOnWrite;
    private final boolean deleteOldestOnOverflow;

    private StateFile state;
    private DataFileReader reader;
    private DataFileWriter writer;

    public ByteQueue(Path directory, long maxSize) throws IOException {
        this(directory, maxSize, true, true, true);
    }

    public ByteQueue(Path directory, long maxSize, boolean flushOnPop, boolean flushOnPush, boolean deleteOldestOnOverflow) throws IOException {
        this.directory = directory;
        this.maxSize = Math.max(Math.min(maxSize, StateFile.MAX_QUEUE_SIZE), StateFile.MIN_QUEUE_SIZE);
        this.dataFileLimit = Math.max(512, this.maxSize / StateFile.MAX_FILES + (this.maxSize % StateFile.MAX_FILES > 0 ? 1 : 0));
        this.lenient = new Lenient(this);

        this.flushOnRead = flushOnPop;
        this.flushOnWrite = flushOnPush;
        this.deleteOldestOnOverflow = deleteOldestOnOverflow;

        reopen();
    }

    public synchronized void reopen() throws IOException {
        close();
        Files.createDirectories(this.directory);
        this.state = new StateFile(this.directory.resolve("state"));
        this.writer = openWriter();
        this.reader = null;
        gc();
    }

    private synchronized void ensureOpen() {
        if (state == null)
            throw new IllegalStateException("This queue is already closed.");
    }

    public synchronized long bytes() {
        ensureOpen();
        return state.getBytes();
    }

    public synchronized long count() {
        ensureOpen();
        return state.getCount();
    }

    public synchronized void clear() throws IOException {
        ensureOpen();
        lenient.perform(new Lenient.Op() {
            @Override
            public int call() throws IOException {
                state.clear();
                state.flush();
                reopen();
                return 0;
            }
        });
    }

    public synchronized int pop(Buffer buffer) throws IOException {
        ensureOpen();
        return lenient.perform(new Lenient.Op() {
            @Override
            public int call() throws IOException {
                if (checkReadEOF())
                    return -1;

                int read = reader().read(buffer);
                state.addReadCount(read);
                if (flushOnRead)
                    state.flush();

                checkReadEOF();
                return buffer.count();
            }
        });
    }

    private boolean deleteOldestFile() throws IOException {
        int currentFile = state.getReadFile();
        state.advanceReadFile(reader().size());
        reader.close();
        state.flush();
        reader = null;
        tryDeleteFile(currentFile);

        return true;
    }


    public synchronized boolean push(Buffer buffer) throws IOException {
        ensureOpen();
        return lenient.perform(new Lenient.Op() {
            @Override
            public int call() throws IOException {
                checkWriteEOF();
                if (checkFutureQueueOverflow(buffer))
                    return 0;

                int written = writer().write(buffer);
                state.addWriteCount(written);
                if (flushOnWrite)
                    state.flush();

                checkWriteEOF();
                return 1;
            }
        }) != 0;
    }

    private boolean checkFutureQueueOverflow(Buffer buffer) throws IOException {
        if (deleteOldestOnOverflow) {
            while (!state.sameFileReadWrite() && willOverflow(buffer))
                deleteOldestFile();
            return false;
        } else {
            return willOverflow(buffer);
        }
    }

    public void flush() throws IOException {
        state.flush();
    }

    public synchronized void close() {
        lenient.safeClose(reader);
        reader = null;

        lenient.safeClose(writer);
        writer = null;

        lenient.safeClose(state);
        state = null;
    }

    private boolean willOverflow(Buffer buffer) {
        return bytes() + buffer.count() + DataFileWriter.OVERHEAD > maxSize;
    }

    private boolean checkReadEOF() throws IOException {
        while (!state.sameFileReadWrite() && reader().eof())
            deleteOldestFile();
        return state.getCount() == 0;
    }

    private DataFileReader reader() throws IOException {
        return reader != null ? reader : (reader = openReader());
    }

    private void checkWriteEOF() throws IOException {
        if (state.getWritePosition() >= dataFileLimit)
            advanceWriteFile();
    }

    private DataFileWriter writer() throws IOException {
        return writer != null ? writer : (writer = openWriter());
    }

    private void advanceWriteFile() throws IOException {
        writer().close();
        state.advanceWriteFile();
        state.flush();
        writer = null;
    }

    private void gc() throws IOException {
        Path file = makeDataPath(state.getReadFile());
        boolean shouldFlush = false;
        while (!Files.exists(file) && state.getReadFile() != state.getWriteFile()) {
            state.advanceReadFile(0);
            file = makeDataPath(state.getReadFile());
            shouldFlush = true;
        }
        long totalBytes = 0;
        long totalCount = 0;
        for (int i = 0; i < StateFile.MAX_FILES; i++) {
            Path path = makeDataPath(i);
            if (Files.exists(path)) {
                if (!state.isInUse(i)) {
                    tryDeleteFile(i);
                } else {
                    totalBytes += Files.size(path);
                    totalCount += state.getFileCount(i);
                }
            }
        }

        shouldFlush |= state.fixCounts(totalCount, totalBytes);

        if (shouldFlush)
            state.flush();

    }

    private void tryDeleteFile(int file) {
        try {
            Files.delete(makeDataPath(file));
        } catch (Exception ignored) {
            //there is no problem in not being able to delete some files
        }
    }

    private Path makeDataPath(int state) {
        return directory.resolve("data" + String.format("%02x", state));
    }

    private DataFileReader openReader() throws IOException {
        return new DataFileReader(makeDataPath(state.getReadFile()), state.getReadPosition());
    }

    private DataFileWriter openWriter() throws IOException {
        Files.createDirectories(directory);
        return new DataFileWriter(makeDataPath(state.getWriteFile()), state.getWritePosition());
    }
}
