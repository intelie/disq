package net.intelie.disq;

import com.google.common.base.Strings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DiskRawQueueTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testPushOnClosedQueue() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 1000);
        queue.close();

        assertThatThrownBy(() -> {
            push(queue, "abc");
        }).isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
    }

    @Test
    public void testExceptionOnClose() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 1000);
        for (String file : temp.getRoot().list()) {
            new File(temp.getRoot(), file).delete();
        }
        temp.getRoot().delete();
        queue.close();

        assertThatThrownBy(() -> {
            push(queue, "abc");
        }).isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
    }

    @Test
    public void testSimplePushsAndPops() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 1000);

        for (int i = 0; i < 20; i++) {
            String s = "test" + String.format("%02x", i);

            push(queue, s);
            assertThat(new File(temp.getRoot(), "data00").length()).isEqualTo((i + 1) * 10);
            assertStateFile(0, 0, i * 10, (i + 1) * 10, 1, (i + 1) * 10, 1, 0, 0);

            assertThat(pop(queue)).isEqualTo(s);
            assertStateFile(0, 0, (i + 1) * 10, (i + 1) * 10, 0, (i + 1) * 10, 0, 0, 0);
        }
    }

    @Test
    public void testSimplePushsAndPopsNoFlush() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 1000, false, false, true);
        queue.flush();

        for (int i = 0; i < 20; i++) {
            String s = "test" + String.format("%02x", i);

            push(queue, s);
            assertThat(new File(temp.getRoot(), "data00").length()).isEqualTo((i + 1) * 10);
            assertStateFile(0, 0, 0, 0, 0, 0, 0, 0, 0);

            assertThat(pop(queue)).isEqualTo(s);
            assertStateFile(0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        queue.flush();
        assertStateFile(0, 0, 200, 200, 0, 200, 0, 0, 0);

    }

    private void assertStateFile(int readFile, int writeFile, int readPosition, int writePosition, int count, int bytes, int c1, int c2, int c3) throws IOException {
        DataInputStream data = new DataInputStream(new FileInputStream(new File(temp.getRoot(), "state")));
        assertThat(data.readShort()).isEqualTo((short) readFile);
        assertThat(data.readShort()).isEqualTo((short) writeFile);
        assertThat(data.readInt()).isEqualTo(readPosition);
        assertThat(data.readInt()).isEqualTo(writePosition);
        assertThat(data.readLong()).isEqualTo(count);
        assertThat(data.readLong()).isEqualTo(bytes);

        assertThat(data.readInt()).isEqualTo(c1);
        assertThat(data.readInt()).isEqualTo(c2);
        assertThat(data.readInt()).isEqualTo(c3);
        for (int i = 3; i < StateFile.MAX_FILES; i++) {
            assertThat(data.readInt()).isEqualTo(0);
        }
        assertThat(data.read()).isEqualTo(-1);
    }


    @Test
    public void testPushAndCloseThenOpenAndPop() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 1000);

        for (int i = 0; i < 20; i++)
            push(queue, "test" + i);
        for (int i = 0; i < 10; i++)
            assertThat(pop(queue)).isEqualTo("test" + i);
        queue.close();
        queue.reopen();
        for (int i = 10; i < 20; i++)
            assertThat(pop(queue)).isEqualTo("test" + i);

        assertThat(queue.count()).isEqualTo(0);
    }

    @Test
    public void testLimitByMaxSize() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512 * 121);

        String s = Strings.repeat("a", 508);

        for (int i = 0; i < 121; i++)
            push(queue, s);

        assertThat(queue.count()).isEqualTo(121);
        assertThat(queue.bytes()).isEqualTo(512 * 121);
        assertThat(queue.remainingBytes()).isEqualTo(0);
        assertThat(queue.remainingCount()).isEqualTo(0);

        assertThat(push(queue, s)).isTrue();

        assertThat(queue.count()).isEqualTo(121);
        assertThat(queue.bytes()).isEqualTo(512 * 121);
    }

    @Test
    public void testRemaningBytes() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512 * 121);

        String s = Strings.repeat("a", 508);

        for (int i = 0; i < 60; i++)
            push(queue, s);

        assertThat(queue.count()).isEqualTo(60);
        assertThat(queue.bytes()).isEqualTo(512 * 60);
        assertThat(queue.remainingBytes()).isEqualTo(61 * 512);
        assertThat(queue.remainingCount()).isEqualTo(61);
    }

    @Test
    public void testLimitByMaxSizeNoOverflow() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512 * 121, true, true, false);

        String s = Strings.repeat("a", 508);

        for (int i = 0; i < 121; i++)
            push(queue, s);

        assertThat(queue.count()).isEqualTo(121);
        assertThat(queue.bytes()).isEqualTo(512 * 121);

        assertThat(push(queue, s)).isFalse();

        assertThat(queue.count()).isEqualTo(121);
        assertThat(queue.bytes()).isEqualTo(512 * 121);
    }

    @Test
    public void testLimitByMaxSizeOnlyTwoFiles() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512 * 121);

        String s = Strings.repeat("a", 512 * 100);

        push(queue, s);
        assertThat(queue.count()).isEqualTo(1);
        assertThat(queue.bytes()).isEqualTo(4 + 512 * 100);

        push(queue, s);
        assertThat(queue.count()).isEqualTo(1);
        assertThat(queue.bytes()).isEqualTo(4 + 512 * 100);
    }

    @Test
    public void testLimitByMaxSizeOnlyOneFile() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512 * 121);

        String s = Strings.repeat("a", 512 * 100);

        push(queue, s);
        assertThat(queue.count()).isEqualTo(1);
        assertThat(queue.bytes()).isEqualTo(4 + 512 * 100);

        push(queue, s);
        assertThat(queue.count()).isEqualTo(1);
        assertThat(queue.bytes()).isEqualTo(4 + 512 * 100);
    }

    @Test
    public void testPeek() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512 * 121);

        String s = Strings.repeat("a", 512);
        push(queue, s);

        assertThat(peek(queue)).isEqualTo(s);
        assertThat(peek(queue)).isEqualTo(s);

        assertThat(pop(queue)).isEqualTo(s);

        assertThat(peek(queue)).isEqualTo(null);
    }


    @Test
    public void testPopOnly() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 512);

        for (int i = 0; i < 3; i++)
            push(queue, s);

        Buffer buffer = new Buffer();

        for (int i = 0; i < 3; i++) {
            assertThat(queue.pop(buffer)).isTrue();
            assertThat(buffer.count()).isEqualTo(512);
        }
        assertThat(queue.pop(buffer)).isFalse();
    }

    private void assertBytesAndCount(DiskRawQueue queue, int bytes, int count) {
        assertThat(queue.bytes()).isEqualTo(bytes);
        assertThat(queue.count()).isEqualTo(count);
    }

    @Test
    public void testSpanningMultipleFiles() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 512);

        for (int i = 0; i < 5; i++)
            push(queue, s);


        assertThat(temp.getRoot().list()).containsOnly(
                "data00", "data01", "data02", "data03", "data04", "state"
        );

        for (int i = 0; i < 5; i++)
            assertThat(new File(temp.getRoot(), "data0" + i).length()).isEqualTo(516);

        for (int i = 0; i < 5; i++)
            assertThat(pop(queue)).isEqualTo(s);

        assertThat(temp.getRoot().list()).containsOnly("state");
        assertThat(queue.bytes()).isEqualTo(0);
    }

    @Test
    public void testAbleToRecoverOnDirectoryDelete() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 512);

        for (int i = 0; i < 5; i++)
            push(queue, s);

        for (String file : temp.getRoot().list()) {
            new File(temp.getRoot(), file).delete();
        }

        temp.getRoot().delete();

        push(queue, s);
        assertThat(pop(queue)).isNull();
        push(queue, "abc");
        assertThat(pop(queue)).isEqualTo("abc");
        assertBytesAndCount(queue, 7, 0);
    }

    @Test
    public void testDeleteAllFiles() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 512);

        for (int i = 0; i < 5; i++)
            push(queue, s);

        for (int i = 0; i < 5; i++)
            assertThat(new File(temp.getRoot(), "data0" + i).length()).isEqualTo(516);

        queue.clear();

        assertBytesAndCount(queue, 0, 0);
        assertThat(temp.getRoot().list()).containsOnly("state", "data00");
    }

    @Test
    public void testAbleToRecoverOnDataFilesDelete() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 512);

        for (int i = 0; i < 5; i++)
            push(queue, s);
        push(queue, "aaa");

        for (String file : temp.getRoot().list()) {
            if (file.startsWith("data"))
                new File(temp.getRoot(), file).delete();
        }

        push(queue, s);
        assertThat(pop(queue)).isNull();
        push(queue, "abc");
        assertThat(pop(queue)).isEqualTo("abc");
        assertBytesAndCount(queue, 7, 0);
        assertThat(temp.getRoot().list()).containsOnly("state", "data06");
    }

    @Test
    public void testAbleToRecoverOnDataFilesMadeReadOnly() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 512);

        for (int i = 0; i < 5; i++)
            push(queue, s);
        push(queue, "aaa");

        temp.getRoot().setWritable(false);

        for (int i = 0; i < 5; i++)
            assertThat(pop(queue)).isEqualTo(s);
        assertThat(pop(queue)).isEqualTo("aaa");
        assertThat(temp.getRoot().list()).containsOnly("data00", "data01", "data02", "data03", "data04", "state", "data05");

        temp.getRoot().setWritable(true);
        queue.reopen();
        assertThat(temp.getRoot().list()).containsOnly("state", "data05");
    }

    private boolean push(DiskRawQueue queue, String s) throws IOException {
        return queue.push(new Buffer(s.getBytes()));
    }

    private String pop(DiskRawQueue queue) throws IOException {
        Buffer buffer = new Buffer();
        if (!queue.pop(buffer)) return null;
        return new String(buffer.buf(), 0, buffer.count());
    }

    private String peek(DiskRawQueue queue) throws IOException {
        Buffer buffer = new Buffer();
        if (!queue.peek(buffer)) return null;
        return new String(buffer.buf(), 0, buffer.count());
    }
}