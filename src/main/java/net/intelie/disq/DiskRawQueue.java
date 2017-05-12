package net.intelie.disq;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DiskRawQueue implements RawQueue {
    private final long maxSize;
    private final long dataFileLimit;
    private final Lenient lenient;
    private final boolean flushOnRead;

    private final boolean flushOnWrite;
    private final boolean deleteOldestOnOverflow;

    private boolean temp;
    private Path directory;
    private boolean closed = false;
    private StateFile state;
    private DataFileReader reader;
    private DataFileWriter writer;

    public DiskRawQueue(Path directory, long maxSize) {
        this(directory, maxSize, true, true, true);
    }

    public DiskRawQueue(Path directory, long maxSize, boolean flushOnPop, boolean flushOnPush, boolean deleteOldestOnOverflow) {
        this.directory = directory;
        this.maxSize = Math.max(Math.min(maxSize, StateFile.MAX_QUEUE_SIZE), StateFile.MIN_QUEUE_SIZE);
        this.dataFileLimit = Math.max(512, this.maxSize / StateFile.MAX_FILES + (this.maxSize % StateFile.MAX_FILES > 0 ? 1 : 0));
        this.lenient = new Lenient(this);

        this.flushOnRead = flushOnPop;
        this.flushOnWrite = flushOnPush;
        this.deleteOldestOnOverflow = deleteOldestOnOverflow;
        this.temp = false;

        reopen();
    }

    @Override
    public synchronized void reopen() {
        internalClose();
        closed = false;
    }

    public Path path() {
        return directory;
    }

    private void internalOpen() throws IOException {
        internalClose();
        if (this.directory == null) {
            this.directory = Files.createTempDirectory("disq");
            this.temp = true;
        }
        Files.createDirectories(this.directory);
        this.state = new StateFile(this.directory.resolve("state"));
        this.writer = openWriter();
        this.reader = null;
        gc();
    }

    public synchronized void touch() throws IOException {
        if (state == null)
            internalOpen();
    }

    private void checkNotClosed() {
        if (closed)
            throw new IllegalStateException("This queue is already closed.");
    }

    @Override
    public synchronized long bytes() {
        checkNotClosed();
        return lenient.performSafe(() -> state.getBytes(), 0);
    }

    @Override
    public synchronized long count() {
        checkNotClosed();

        return lenient.performSafe(() -> state.getCount(), 0);
    }

    @Override
    public synchronized long remainingBytes() {
        checkNotClosed();

        return lenient.performSafe(() -> maxSize - state.getBytes(), 0);
    }

    @Override
    public synchronized long remainingCount() {
        checkNotClosed();

        return lenient.performSafe(() -> {
            if (state.getCount() == 0) return maxSize / 4;
            double bytesPerElement = state.getBytes() / (double) state.getCount();
            return (long) ((maxSize - state.getBytes()) / bytesPerElement);
        }, 0);
    }

    @Override
    public synchronized void clear() throws IOException {
        checkNotClosed();

        lenient.perform(() -> {
            state.clear();
            state.flush();
            reopen();
            return 1;
        });
    }

    @Override
    public synchronized boolean pop(Buffer buffer) throws IOException {
        checkNotClosed();

        return lenient.perform(() -> {
            if (checkReadEOF())
                return 0;

            int read = reader().read(buffer);
            state.addReadCount(read);
            if (flushOnRead)
                state.flush();

            checkReadEOF();
            return 1;
        }) > 0;
    }

    @Override
    public synchronized boolean peek(Buffer buffer) throws IOException {
        checkNotClosed();

        return lenient.perform(() -> {
            if (checkReadEOF())
                return 0;

            reader().peek(buffer);
            return 1;
        }) > 0;
    }

    private void deleteOldestFile() throws IOException {
        int currentFile = state.getReadFile();
        state.advanceReadFile(reader().size());
        reader.close();
        state.flush();
        reader = null;
        tryDeleteFile(currentFile);
    }


    @Override
    public synchronized boolean push(Buffer buffer) throws IOException {
        checkNotClosed();

        return lenient.perform(() -> {
            checkWriteEOF();
            if (checkFutureQueueOverflow(buffer))
                return 0;

            int written = writer().write(buffer);
            state.addWriteCount(written);
            if (flushOnWrite)
                state.flush();

            checkWriteEOF();
            return 1;
        }) > 0;
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

    @Override
    public void flush() throws IOException {
        checkNotClosed();

        lenient.perform(() -> {
            state.flush();
            return 1;
        });
    }

    @Override
    public synchronized void close() {
        closed = true;
        internalClose();
    }

    private void internalClose() {
        lenient.safeClose(reader);
        reader = null;

        lenient.safeClose(writer);
        writer = null;

        lenient.safeClose(state);
        state = null;

        if (temp) {
            lenient.safeDelete(directory);
            directory = null;
            temp = false;
        }
    }

    private boolean willOverflow(Buffer buffer) throws IOException {
        return bytes() + buffer.count() + DataFileWriter.OVERHEAD > maxSize;
    }

    private boolean checkReadEOF() throws IOException {
        while (!state.sameFileReadWrite() && state.readFileEof())
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
