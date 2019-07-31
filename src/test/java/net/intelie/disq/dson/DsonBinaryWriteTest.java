package net.intelie.disq.dson;

import net.intelie.disq.Buffer;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class DsonBinaryWriteTest {
    @Test
    public void testWriteLatin1() throws IOException {
        Buffer buf = new Buffer();
        DsonBinaryWrite.writeLatin1(buf.write(), "ação");
        Latin1View view = new Latin1View();
        DsonBinaryRead.readLatin1(buf.read(), view);
        assertThat(view.toString()).isEqualTo("ação");
        assertThat(view.subSequence(1, 3).toString()).isEqualTo("çã");
    }

    @Test
    public void testWriteString() throws IOException {
        Buffer buf = new Buffer();
        DsonBinaryWrite.writeUnicode(buf.write(), "ação");
        UnicodeView view = new UnicodeView();
        DsonBinaryRead.readUnicode(buf.read(), view);
        assertThat(view.toString()).isEqualTo("ação");
        assertThat(view.subSequence(1, 3).toString()).isEqualTo("çã");
    }

    @Test
    public void testWriteCrazyString() throws IOException {
        Buffer buf = new Buffer();
        DsonBinaryWrite.writeUnicode(buf.write(), "(╯°□°)╯︵ ┻━┻");
        UnicodeView view = new UnicodeView();
        DsonBinaryRead.readUnicode(buf.read(), view);
        assertThat(view.toString()).isEqualTo("(╯°□°)╯︵ ┻━┻");
        assertThat(view.subSequence(1, 5).toString()).isEqualTo("╯°□°");
    }

    @Test
    public void testWriteLong() throws IOException {
        for (long i = 3; i < Long.MAX_VALUE / 3; i *= 3) {
            Buffer buf = new Buffer();
            DsonBinaryWrite.writeInt64(buf.write(), i);
            assertThat(DsonBinaryRead.readInt64(buf.read())).isEqualTo(i);
        }
    }

    @Test
    public void testWriteDouble() throws IOException {
        for (double i = Double.MIN_VALUE; i < Double.MAX_VALUE; i *= 3) {
            Buffer buf = new Buffer();
            DsonBinaryWrite.writeNumber(buf.write(), i);
            assertThat(DsonBinaryRead.readNumber(buf.read())).isEqualTo(i);
        }
    }

    @Test
    public void testWriteInt() throws IOException {
        for (int i = 3; i < Integer.MAX_VALUE / 3; i *= 3) {
            Buffer buf = new Buffer();
            DsonBinaryWrite.writeInt32(buf.write(), i);
            assertThat(DsonBinaryRead.readInt32(buf.read())).isEqualTo(i);
        }
    }
}