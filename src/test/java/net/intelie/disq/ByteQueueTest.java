package net.intelie.disq;

import com.google.common.base.Strings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

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