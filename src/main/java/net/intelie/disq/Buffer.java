package net.intelie.disq;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class Buffer {
    private byte[] buf;
    private int count;

    public Buffer() {
        this(32);
    }

    public Buffer(int initialCapacity) {
        this(new byte[initialCapacity], 0);
    }

    public Buffer(byte[] buf) {
        this(buf, buf.length);
    }

    private Buffer(byte[] buf, int count) {
        this.buf = buf;
        this.count = count;
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

    public void setCountAtLeast(int newCount, boolean preserve) {
        if (newCount > count) {
            setCount(newCount, preserve);
        }
    }

    public void setCount(int newCount, boolean preserve) {
        ensureCapacity(newCount, preserve);
        count = newCount;
    }

    public void ensureCapacity(int capacity, boolean preserve) {
        int newCapacity = (1 << (Integer.SIZE - Integer.numberOfLeadingZeros(capacity) - 1));
        if (newCapacity < capacity) newCapacity <<= 1;

        if (preserve) buf = Arrays.copyOf(buf, newCapacity);
        else buf = new byte[newCapacity];
    }

    public OutputStream write() {
        return write(0);
    }

    public OutputStream write(int start) {
        return new Out(start);
    }

    public InputStream read() {
        return read(0);
    }

    public InputStream read(int start) {
        return new In(start);
    }

    private class Out extends OutputStream {
        private int position;

        public Out(int start) {
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

    private class In extends InputStream {
        private int marked = 0;
        private int position = 0;

        public In(int start) {
            position = marked = start;
        }

        @Override
        public int read() throws IOException {
            if (position + 1 > count)
                return -1;
            return buf[position++];
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
