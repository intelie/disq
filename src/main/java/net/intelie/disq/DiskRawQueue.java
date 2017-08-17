package net.intelie.disq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DiskRawQueue implements RawQueue {
    public static final int FAILED_READ_THRESHOLD = 64;
    private static final Logger LOGGER = LoggerFactory.getLogger(DiskRawQueue.class);
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
    private int failedReads = 0, oldFailedReads = 0;

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
        this.writer = null;
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

    public synchronized long files() {
        checkNotClosed();

        return lenient.performSafe(() -> state.getNumberOfFiles(), 0);
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
            internalFlush();
            reopen();
            return 1;
        });
    }

    @Override
    public synchronized boolean pop(Buffer buffer) throws IOException {
        checkNotClosed();

        return lenient.perform(() -> {
            if (!checkFailedReads())
                return 0;

            if (checkReadEOF())
                return 0;

            int read = innerRead(buffer);

            state.addReadCount(read);
            if (flushOnRead)
                internalFlush();

            checkReadEOF();
            return 1;
        }) > 0;
    }

    private boolean checkFailedReads() throws IOException {
        if (failedReads >= FAILED_READ_THRESHOLD) {
            LOGGER.info("Detected corrupted file #{}, backing up and moving on.", state.getReadFile());
            boolean wasSame = state.sameFileReadWrite();
            deleteOldestFile(true);
            if (wasSame) {
                clear();
                return false;
            }
        }
        return true;
    }

    private int innerRead(Buffer buffer) throws IOException {
        try {
            int read = reader().read(buffer);
            oldFailedReads = failedReads;
            failedReads = 0;
            return read;
        } catch (Throwable e) {
            failedReads++;
            throw e;
        }
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

    private void deleteOldestFile(boolean renameFile) throws IOException {
        int currentFile = state.getReadFile();
        state.advanceReadFile(reader().size());
        reader.close();
        failedReads = 0;

        internalFlush();
        reader = null;
        tryDeleteFile(currentFile, renameFile);
    }


    @Override
    public synchronized void notifyFailedRead() {
        failedReads = oldFailedReads + 1;
    }

    @Override
    public synchronized boolean push(Buffer buffer) throws IOException {
        checkNotClosed();

        return lenient.perform(() -> {
            checkWriteEOF();
            if (checkFutureQueueOverflow(buffer.count()))
                return 0;

            int written = writer().write(buffer);
            state.addWriteCount(written);
            if (flushOnWrite)
                internalFlush();

            checkWriteEOF();
            return 1;
        }) > 0;
    }

    private boolean checkFutureQueueOverflow(int count) throws IOException {
        if (deleteOldestOnOverflow) {
            while (!state.sameFileReadWrite() && willOverflow(count))
                deleteOldestFile(false);
            return false;
        } else {
            return willOverflow(count);
        }
    }

    @Override
    public void flush() throws IOException {
        checkNotClosed();

        lenient.perform(this::internalFlush);
    }

    private long internalFlush() throws IOException {
        if (writer != null)
            writer.flush();
        state.flush();
        return 1;
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

    private boolean willOverflow(int count) throws IOException {
        return bytes() + count + DataFileWriter.OVERHEAD > maxSize || files() >= StateFile.MAX_FILES;
    }

    private boolean checkReadEOF() throws IOException {
        while (!state.sameFileReadWrite() && state.readFileEof())
            deleteOldestFile(false);
        if (state.needsFlushBeforePop())
            internalFlush();
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
        internalFlush();
        writer = null;
    }

    private void gc() throws IOException {
        Path file = makeDataPath(state.getReadFile());
        boolean shouldFlush = false;
        while (!Files.exists(file) && !state.sameFileReadWrite()) {
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
                    tryDeleteFile(i, false);
                } else {
                    totalBytes += Files.size(path);
                    totalCount += state.getFileCount(i);
                }
            }
        }

        shouldFlush |= state.fixCounts(totalCount, totalBytes);

        if (shouldFlush)
            internalFlush();

    }

    private void tryDeleteFile(int file, boolean renameFile) {
        Path from = makeDataPath(file);
        try {
            if (renameFile) {
                Path to = makeCorruptedPath(file);
                LOGGER.info("Backing up {} as {}", from, to);
                Files.move(from, to);
            } else {
                Files.delete(from);
            }
        } catch (Exception e) {
            LOGGER.info("Unable to delete file {}", from);
            LOGGER.info("Stacktrace", e);
        }
    }

    private Path makeDataPath(int state) {
        return directory.resolve(String.format("data%02x", state));
    }

    private Path makeCorruptedPath(int state) {
        return directory.resolve(String.format("data%02x.%d.corrupted", state, System.currentTimeMillis()));
    }

    private DataFileReader openReader() throws IOException {
        return new DataFileReader(makeDataPath(state.getReadFile()), state.getReadPosition());
    }

    private DataFileWriter openWriter() throws IOException {
        Files.createDirectories(directory);
        return new DataFileWriter(makeDataPath(state.getWriteFile()), state.getWritePosition());
    }
}
