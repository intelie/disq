package net.intelie.disq;

import com.google.common.base.Strings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

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
            assertStateFile(temp.getRoot(), 0, 0, i * 10, (i + 1) * 10, 1, (i + 1) * 10, 1, 0, 0);

            assertThat(pop(queue)).isEqualTo(s);
            assertStateFile(temp.getRoot(), 0, 0, (i + 1) * 10, (i + 1) * 10, 0, (i + 1) * 10, 0, 0, 0);
        }
    }

    @Test
    public void testSimplePushsAndPopsOnTemporaryDir() throws Exception {
        Path saved;
        try (DiskRawQueue queue = new DiskRawQueue(null, 1000)) {
            assertThat(queue.path()).isNull();

            for (int i = 0; i < 20; i++) {
                String s = "test" + String.format("%02x", i);

                push(queue, s);
                assertThat(new File(queue.path().toFile(), "data00").length()).isEqualTo((i + 1) * 10);
                assertStateFile(queue.path().toFile(), 0, 0, i * 10, (i + 1) * 10, 1, (i + 1) * 10, 1, 0, 0);

                assertThat(pop(queue)).isEqualTo(s);
                assertStateFile(queue.path().toFile(), 0, 0, (i + 1) * 10, (i + 1) * 10, 0, (i + 1) * 10, 0, 0, 0);
            }

            assertThat(saved = queue.path()).isNotNull();
            assertThat(Files.exists(saved)).isTrue();
        }
        assertThat(Files.exists(saved)).isFalse();
    }

    @Test
    public void testSimplePushsAndPopsOnTemporaryDirCantDelete() throws Exception {
        Path saved;
        try (DiskRawQueue queue = new DiskRawQueue(null, 1000)) {
            assertThat(queue.path()).isNull();

            for (int i = 0; i < 20; i++) {
                String s = "test" + String.format("%02x", i);

                push(queue, s);
                assertThat(new File(queue.path().toFile(), "data00").length()).isEqualTo((i + 1) * 10);
                assertStateFile(queue.path().toFile(), 0, 0, i * 10, (i + 1) * 10, 1, (i + 1) * 10, 1, 0, 0);

                assertThat(pop(queue)).isEqualTo(s);
                assertStateFile(queue.path().toFile(), 0, 0, (i + 1) * 10, (i + 1) * 10, 0, (i + 1) * 10, 0, 0, 0);
            }

            assertThat(saved = queue.path()).isNotNull();
            assertThat(Files.exists(saved)).isTrue();

            queue.path().toFile().setWritable(false);
        }
        assertThat(Files.exists(saved)).isTrue();
        saved.toFile().setWritable(true);
        new Lenient(null).safeDelete(saved);
        assertThat(Files.exists(saved)).isFalse();
    }

    @Test
    public void testSimplePushsAndPopsNoFlush() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 1000, false, false, true);
        queue.flush();

        for (int i = 0; i < 20; i++) {
            String s = "test" + String.format("%02x", i);

            push(queue, s);
            assertThat(new File(temp.getRoot(), "data00").length()).isEqualTo(10 * i);
            assertStateFile(queue.path().toFile(), 0, 0, Math.max(0, (i - 1) * 10), i * 10, Math.min(i, 1), Math.max(0, i * 10), Math.min(i, 1), 0, 0);

            assertThat(pop(queue)).isEqualTo(s);
            assertThat(new File(temp.getRoot(), "data00").length()).isEqualTo(10 * (i + 1));

            assertStateFile(queue.path().toFile(), 0, 0, i * 10, (i + 1) * 10, 1, (i + 1) * 10, 1, 0, 0);
        }

        queue.flush();
        assertStateFile(temp.getRoot(), 0, 0, 200, 200, 0, 200, 0, 0, 0);

    }

    private void assertStateFile(File root, int readFile, int writeFile, int readPosition, int writePosition, int count, int bytes, int c1, int c2, int c3) throws IOException {
        DataInputStream data = new DataInputStream(new FileInputStream(new File(root, "state")));
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
        assertThat(queue.files()).isEqualTo(121);
        assertThat(queue.bytes()).isEqualTo(512 * 121);
        assertThat(queue.remainingBytes()).isEqualTo(0);
        assertThat(queue.remainingCount()).isEqualTo(0);

        assertThat(push(queue, s)).isTrue();

        assertThat(queue.count()).isEqualTo(121);
        assertThat(queue.bytes()).isEqualTo(512 * 121);

        queue.reopen();
        for (int i = 0; i < 121; i++)
            assertThat(pop(queue)).isEqualTo(s);
    }

    @Test
    public void testLimitByNumberOfFiles() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512 * 121);

        String s1 = Strings.repeat("a", 508);
        for (int i = 0; i < 110; i++)
            push(queue, s1);

        queue.close();
        queue = new DiskRawQueue(temp.getRoot().toPath(), 1024 * 121);

        String s2 = Strings.repeat("a", 1020);
        for (int i = 0; i < 11; i++)
            push(queue, s2);

        assertThat(queue.files()).isEqualTo(121);
        assertThat(queue.count()).isEqualTo(121);
        assertThat(queue.bytes()).isEqualTo(512 * 110 + 1024 * 11);

        push(queue, s2);
        queue.reopen();

        assertThat(queue.files()).isEqualTo(121);
        assertThat(queue.count()).isEqualTo(121);
        assertThat(queue.bytes()).isEqualTo(512 * 109 + 1024 * 12);

        for (int i = 0; i < 109; i++)
            assertThat(pop(queue)).isEqualTo(s1);
        for (int i = 0; i < 12; i++)
            assertThat(pop(queue)).isEqualTo(s2);
        assertThat(pop(queue)).isEqualTo(null);
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
        assertThat(temp.getRoot().list()).containsOnly("state");

        for (int i = 0; i < 5; i++)
            push(queue, s);
        for (int i = 0; i < 5; i++)
            assertThat(new File(temp.getRoot(), "data0" + i).length()).isEqualTo(516);
        assertBytesAndCount(queue, 5 * 516, 5);
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
        queue.touch();
        assertThat(temp.getRoot().list()).containsOnly("state", "data05");
    }

    @Test
    public void testAbleToDetectCorruptedFiles() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 512);

        for (int i = 0; i < 5; i++)
            push(queue, s);

        try (FileWriter writer = new FileWriter(new File(temp.getRoot(), "data00"))) {
            writer.write("abcdeqwefwefwewefger");
        }

        for (int i = 0; i < 32; i++)
            assertThatThrownBy(() -> pop(queue))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Buffer overflowed");

        for (int i = 0; i < 4; i++)
            assertThat(pop(queue)).isEqualTo(s);
        String[] files = temp.getRoot().list();
        Arrays.sort(files);
        assertThat(files.length).isEqualTo(2);
        assertThat(files).contains("state");
        assertThat(files[0]).startsWith("data00").endsWith(".corrupted");
    }

    @Test
    public void testAbleToDetectSingleCorruptedFile() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 100);

        push(queue, s);

        try (FileWriter writer = new FileWriter(new File(temp.getRoot(), "data00"))) {
            writer.write("abcdeqwefwefwewefger");
        }

        for (int i = 0; i < 32; i++)
            assertThatThrownBy(() -> pop(queue))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Buffer overflowed");

        push(queue, s);
        assertThat(pop(queue)).isEqualTo(null);
        push(queue, s);
        assertThat(pop(queue)).isEqualTo(s);
        assertThat(pop(queue)).isEqualTo(null);

        String[] files = temp.getRoot().list();
        Arrays.sort(files);
        assertThat(files.length).isEqualTo(3);
        assertThat(files).contains("state");
        assertThat(files[1]).startsWith("data00").endsWith(".corrupted");
    }

    @Test
    public void testAbleToDetectCorruptedFileManyPerFile() throws Exception {
        DiskRawQueue queue = new DiskRawQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 100);

        for (int i = 0; i < 20; i++) {
            push(queue, s);
        }

        try (FileWriter writer = new FileWriter(new File(temp.getRoot(), "data00"))) {
            writer.write("abcdeqwefwefwewefger");
        }

        for (int i = 0; i < 32; i++)
            assertThatThrownBy(() -> pop(queue))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Buffer overflowed");


        push(queue, s);
        for (int i = 0; i < 15; i++) {
            assertThat(pop(queue)).isEqualTo(s);
        }


        String[] files = temp.getRoot().list();
        Arrays.sort(files);
        assertThat(files.length).isEqualTo(3);
        assertThat(files).contains("state");
        assertThat(files).contains("data04");
        assertThat(files[0]).startsWith("data00").endsWith(".corrupted");
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