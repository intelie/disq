package net.intelie.disq;

import java.io.ByteArrayOutputStream;

class BufferOutputStream extends ByteArrayOutputStream {
    public byte[] innerBuffer() {
        return buf;
    }
}
