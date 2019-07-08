package net.intelie.disq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class Buffer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Buffer.class);

    private final int maxCapacity;
    private byte[] buf;
    private int count;

    public Buffer() {
        this(-1);
    }

    public Buffer(int maxCapacity) {
        this(32, maxCapacity);
    }

    public Buffer(int initialCapacity, int maxCapacity) {
        this(new byte[initialCapacity], 0, maxCapacity);
    }

    public Buffer(byte[] buf) {
        this(buf, buf.length, buf.length);
    }

    private Buffer(byte[] buf, int count, int maxCapacity) {
        this.buf = buf;
        this.count = count;
        this.maxCapacity = maxCapacity;
    }

    public int currentCapacity() {
        return buf.length;
    }

    public int count() {
        return count;
    }

    public void clear() {
        count = 0;
    }

    public byte[] buf() {
        return buf;
    }

    public byte[] toArray() {
        return Arrays.copyOf(buf, count);
    }

    public void setCountAtLeast(int newCount, boolean preserve) throws IOException {
        if (newCount > count) {
            setCount(newCount, preserve);
        }
    }

    public void setCount(int newCount, boolean preserve) throws IOException {
        ensureCapacity(newCount, preserve);
        count = newCount;
    }

    public void ensureCapacity(int capacity) throws IOException {
        ensureCapacity(capacity, false);
    }

    public void ensureCapacity(int capacity, boolean preserve) throws IOException {
        if (capacity <= buf.length) return;
        int newCapacity = findBestNewCapacity(capacity);

        if (capacity > newCapacity) {
            LOGGER.info("Buffer overflowed. Len={}, Max={}", capacity, maxCapacity);
            throw new IOException("Buffer overflowed: " + capacity + "/" + maxCapacity + " bytes");
        }

        if (preserve) buf = Arrays.copyOf(buf, newCapacity);
        else buf = new byte[newCapacity];
    }

    private int findBestNewCapacity(int capacity) {
        int newCapacity = (1 << (Integer.SIZE - Integer.numberOfLeadingZeros(capacity) - 1));
        if (newCapacity < capacity) newCapacity <<= 1;
        if (maxCapacity >= 0) newCapacity = Math.min(newCapacity, maxCapacity);
        return newCapacity;
    }

    public OutStream write() {
        return write(0);
    }

    public OutStream write(int start) {
        return new OutStream(start);
    }

    public InStream read() {
        return read(0);
    }

    public InStream read(int start) {
        return new InStream(start);
    }

    public class OutStream extends OutputStream {
        private int position;

        public OutStream(int start) {
            position = start;
        }

        @Override
        public void write(int b) throws IOException {
            setCountAtLeast(position + 1, true);
            buf[position++] = (byte) b;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            setCountAtLeast(position + len, true);
            System.arraycopy(b, off, buf, position, len);
            position += len;
        }
    }

    public class InStream extends InputStream {
        private int marked = 0;
        private int position = 0;

        public InStream(int start) {
            position = marked = start;
        }

        @Override
        public int read() throws IOException {
            if (position + 1 > count)
                return -1;
            return buf[position++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (position >= count)
                return -1;
            int toRead = Math.min(count - position, len);
            System.arraycopy(buf, position, b, off, toRead);
            position += toRead;
            return toRead;
        }

        @Override
        public long skip(long n) throws IOException {
            long toSkip = Math.min(count - position, n);
            position += toSkip;
            return toSkip;
        }

        @Override
        public int available() throws IOException {
            return count - position;
        }

        @Override
        public synchronized void mark(int readlimit) {
            marked = position;
        }

        @Override
        public synchronized void reset() throws IOException {
            position = marked;
        }
    }
}
