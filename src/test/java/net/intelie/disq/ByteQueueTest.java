package net.intelie.disq;

import com.google.common.base.Strings;
import org.assertj.core.condition.Negative;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class ByteQueueTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testSimplePushsAndPops() throws Exception {
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 1000);

        for (int i = 0; i < 20; i++) {
            push(queue, "test" + i);
            assertThat(pop(queue)).isEqualTo("test" + i);
        }
    }

    @Test
    public void testSpanningMultipleFiles() throws Exception {
        ByteQueue queue = new ByteQueue(temp.getRoot().toPath(), 512);

        String s = Strings.repeat("a", 512);

        for (int i = 0; i < 5; i++)
            push(queue, s);

        assertThat(temp.getRoot().list()).containsOnly(
                "data00", "data01", "data02", "data03", "data04", "index"
        );

        for (int i = 0; i < 5; i++)
            assertThat(new File(temp.getRoot(), "data0" + i).length()).isEqualTo(516);

        for (int i = 0; i < 5; i++)
            assertThat(pop(queue)).isEqualTo(s);

        assertThat(temp.getRoot().list()).containsOnly("index");
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
        try {
            pop(queue);
            fail("must throw");
        } catch (NegativeArraySizeException e) {
        }
        push(queue, "abc");
        assertThat(pop(queue)).isEqualTo("abc");
        assertThat(queue.bytes()).isEqualTo(7);
        assertThat(queue.count()).isEqualTo(0);
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

        assertThat(queue.bytes()).isEqualTo(0);
        assertThat(queue.count()).isEqualTo(0);
        assertThat(temp.getRoot().list()).containsOnly("index", "data00");
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
        try {
            pop(queue);
            fail("must throw");
        } catch (NegativeArraySizeException e) {
        }
        push(queue, "abc");
        assertThat(pop(queue)).isEqualTo("abc");
        assertThat(queue.bytes()).isEqualTo(7);
        assertThat(queue.count()).isEqualTo(0);
        assertThat(temp.getRoot().list()).containsOnly("index", "data06");
    }

    private void push(ByteQueue queue, String s) throws IOException {
        byte[] bytes = s.getBytes();
        queue.push(bytes, 0, bytes.length);
    }

    private String pop(ByteQueue queue) throws IOException {
        byte[] bytes = new byte[queue.peekNextSize()];
        int read = queue.pop(bytes, 0);
        return new String(bytes);
    }
}