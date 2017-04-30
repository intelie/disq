package net.intelie.disq;

import com.google.common.base.Strings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ByteQueueTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testPushOnClosedQueue() throws Exception {
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 1000);
        queue.close();

        assertThatThrownBy(() -> {
            push(queue, "abc");
        }).isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
    }

    @Test
    public void testExceptionOnClose() throws Exception {
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 1000);
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
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 1000);

        for (int i = 0; i < 20; i++) {
            push(queue, "test" + i);
            assertThat(pop(queue)).isEqualTo("test" + i);
        }
    }

    @Test
    public void testPushAndCloseThenOpenAndPop() throws Exception {
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 1000);

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
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 512 * 121);

        String s = Strings.repeat("a", 508);

        for (int i = 0; i < 121; i++)
            push(queue, s);

        assertThat(queue.count()).isEqualTo(121);
        assertThat(queue.bytes()).isEqualTo(512 * 121);

        push(queue, s);

        assertThat(queue.count()).isEqualTo(121);
        assertThat(queue.bytes()).isEqualTo(512 * 121);
    }

    @Test
    public void testLimitByMaxSizeOnlyTwoFiles() throws Exception {
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 512 * 121);

        String s = Strings.repeat("a", 512 * 100);

        push(queue, s);
        assertThat(queue.count()).isEqualTo(1);
        assertThat(queue.bytes()).isEqualTo(4 + 512*100);

        push(queue, s);
        assertThat(queue.count()).isEqualTo(1);
        assertThat(queue.bytes()).isEqualTo(4 + 512*100);
    }

    @Test
    public void testLimitByMaxSizeOnlyOneFile() throws Exception {
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 512 * 121);

        String s = Strings.repeat("a", 512 * 100);

        push(queue, s);
        assertThat(queue.count()).isEqualTo(1);
        assertThat(queue.bytes()).isEqualTo(4 + 512*100);

        push(queue, s);
        assertThat(queue.count()).isEqualTo(1);
        assertThat(queue.bytes()).isEqualTo(4 + 512*100);
    }


    @Test
    public void testPopOnly() throws Exception {
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 512);

        for (int i = 0; i < 3; i++)
            push(queue, s);

        Buffer buffer = new Buffer();

        assertThat(queue.pop(buffer)).isEqualTo(512);
        assertThat(queue.pop(buffer)).isEqualTo(512);
        assertThat(queue.pop(buffer)).isEqualTo(512);
        assertThat(queue.pop(buffer)).isEqualTo(-1);
    }

    private void assertBytesAndCount(ByteQueue queue, int bytes, int count) {
        assertThat(queue.bytes()).isEqualTo(bytes);
        assertThat(queue.count()).isEqualTo(count);
    }

    @Test
    public void testSpanningMultipleFiles() throws Exception {
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 512);

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
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 512);

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
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 512);

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
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 512);

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
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 512);

        for (int i = 0; i < 5; i++)
            push(queue, s);
        push(queue, "aaa");

        temp.getRoot().setWritable(false);

        for(int i=0; i<5; i++)
            assertThat(pop(queue)).isEqualTo(s);
        assertThat(pop(queue)).isEqualTo("aaa");
        assertThat(temp.getRoot().list()).containsOnly("data00", "data01", "data02", "data03", "data04", "state", "data05");

        temp.getRoot().setWritable(true);
        queue.reopen();
        assertThat(temp.getRoot().list()).containsOnly("state", "data05");
    }

    private void push(ByteQueue queue, String s) throws IOException {
        queue.push(new Buffer(s.getBytes()));
    }

    private String pop(ByteQueue queue) throws IOException {
        Buffer buffer = new Buffer();
        int read = queue.pop(buffer);
        if (read < 0) return null;
        return new String(buffer.buf(), 0, buffer.count());
    }
}