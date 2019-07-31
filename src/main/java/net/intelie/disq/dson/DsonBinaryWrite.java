package net.intelie.disq.dson;

import net.intelie.disq.Buffer;

import java.io.IOException;

public abstract class DsonBinaryWrite {
    public static void writeUnicode(Buffer.OutStream stream, CharSequence str) throws IOException {
        writeInt32(stream, str.length());
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            stream.write((c) & 0xFF);
            stream.write((c >>> 8) & 0xFF);
        }
    }

    public static void writeLatin1(Buffer.OutStream stream, CharSequence str) throws IOException {
        writeInt32(stream, str.length());
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            stream.write((c) & 0xFF);
        }
    }

    public static void writeType(Buffer.OutStream stream, DsonType string) throws IOException {
        stream.write(string.getValue());
    }

    public static void writeNumber(Buffer.OutStream stream, double value) throws IOException {
        writeInt64(stream, Double.doubleToRawLongBits(value));
    }

    public static void writeBoolean(Buffer.OutStream stream, boolean value) throws IOException {
        stream.write(value ? 1 : 0);
    }

    public static void writeInt32(Buffer.OutStream stream, int value) throws IOException {
        stream.write((value) & 0xFF);
        stream.write((value >> 8) & 0xFF);
        stream.write((value >> 16) & 0xFF);
        stream.write((value >> 24) & 0xFF);
    }

    public static void writeInt64(Buffer.OutStream stream, long value) throws IOException {
        stream.write((int) ((value) & 0xFF));
        stream.write((int) ((value >>> 8) & 0xFF));
        stream.write((int) ((value >>> 16) & 0xFF));
        stream.write((int) ((value >>> 24) & 0xFF));
        stream.write((int) ((value >>> 32) & 0xFF));
        stream.write((int) ((value >>> 40) & 0xFF));
        stream.write((int) ((value >>> 48) & 0xFF));
        stream.write((int) ((value >>> 56) & 0xFF));
    }
}
