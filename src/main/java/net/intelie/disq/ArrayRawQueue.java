package net.intelie.disq;

public class ArrayRawQueue implements RawQueue {
    private final byte[] memory;
    private final boolean deleteOldestOnOverflow;
    private int begin = 0, bytes = 0, count = 0;

    public ArrayRawQueue(int maxSize, boolean deleteOldestOnOverflow) {
        this.memory = new byte[maxSize];
        this.deleteOldestOnOverflow = deleteOldestOnOverflow;
    }

    @Override
    public void reopen() {

    }

    @Override
    public void touch() {

    }

    @Override
    public synchronized long bytes() {
        return bytes;
    }

    @Override
    public synchronized long count() {
        return count;
    }

    @Override
    public synchronized long remainingBytes() {
        return memory.length - bytes;
    }

    @Override
    public synchronized long remainingCount() {
        if (count() == 0) return memory.length / 12;
        return (long) (remainingBytes() / (bytes() / (double) count()));
    }

    @Override
    public synchronized void clear() {
        begin = count = bytes = 0;
    }

    @Override
    public synchronized boolean pop(Buffer buffer) {
        if (!peek(buffer)) return false;
        int read = 12 + buffer.count();
        begin = (begin + read) % memory.length;
        bytes -= read;
        count--;
        return true;
    }

    @Override
    public long nextTimestamp() {
        return readLong(0);
    }

    @Override
    public synchronized boolean peek(Buffer buffer) {
        if (bytes == 0) return false;
        long timestamp = readLong(0);
        int size = readInt(8);
        buffer.setCount(size, false);
        buffer.setTimestamp(timestamp);

        int myBegin = (begin + 12) % memory.length;
        int firstSize = Math.min(memory.length - myBegin, size);
        System.arraycopy(memory, myBegin, buffer.buf(), 0, firstSize);

        if (firstSize < size)
            System.arraycopy(memory, 0, buffer.buf(), firstSize, size - firstSize);

        return true;
    }

    @Override
    public synchronized boolean push(Buffer buffer) {
        int size = buffer.count();
        if (size + 12 > memory.length) return false;
        while (deleteOldestOnOverflow && this.bytes + size + 12 > memory.length) {
            int oldSize = readInt(8);
            begin = (begin + 12 + oldSize) % memory.length;
            bytes -= 12 + oldSize;
            count--;
        }
        if (this.bytes + size + 12 > memory.length) return false;

        writeLong(0, buffer.getTimestamp());
        writeInt(8, size);
        int myBegin = (begin + this.bytes + 12) % memory.length;
        int firstSize = Math.min(memory.length - myBegin, size);
        System.arraycopy(buffer.buf(), 0, memory, myBegin, firstSize);
        if (firstSize < size)
            System.arraycopy(buffer.buf(), firstSize, memory, 0, size - firstSize);

        bytes += 12 + buffer.count();
        count++;

        return true;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    private int readInt(int start) {
        int ret = 0;
        for (int i = 0; i < 4; i++) {
            ret <<= 8;
            ret |= (int) memory[(start + begin + i) % memory.length] & 0xFF;
        }
        return ret;
    }

    private void writeInt(int start, int value) {
        for (int i = 0; i < 4; i++) {
            memory[(start + begin + this.bytes + 3 - i) % memory.length] = (byte) (value & 0xFF);
            value >>= 8;
        }
    }

    private long readLong(int start) {
        long ret = 0;
        for (int i = 0; i < 8; i++) {
            ret <<= 8;
            ret |= (long) memory[(start + begin + i) % memory.length] & 0xFF;
        }
        return ret;
    }

    private void writeLong(int start, long value) {
        for (int i = 0; i < 8; i++) {
            memory[(start + begin + this.bytes + 7 - i) % memory.length] = (byte) (value & 0xFFL);
            value >>= 8;
        }
    }
}
