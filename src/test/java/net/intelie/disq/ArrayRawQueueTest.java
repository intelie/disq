package net.intelie.disq;

import com.google.common.base.Strings;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class ArrayRawQueueTest {

    @Test
    public void reopenFlushCloseDoNothing() throws Exception {
        ArrayRawQueue queue = new ArrayRawQueue(10, true);
        queue.reopen();
        queue.touch();
        queue.flush();
        queue.close();
    }

    @Test
    public void testSimplePushsAndPops() throws Exception {
        ArrayRawQueue queue = new ArrayRawQueue(25, true);

        for (int i = 0; i < 20; i++) {
            String s = "test" + i;

            assertThat(push(queue, s, 1000 + i)).isTrue();
            assertThat(queue.bytes()).isGreaterThanOrEqualTo(9);
            assertThat(queue.count()).isEqualTo(1);
            assertThat(queue.remainingBytes()).isLessThanOrEqualTo(10);
            assertThat(queue.remainingCount()).isEqualTo(0);

            assertThat(queue.nextTimestamp()).isEqualTo(1000 + i);
            assertThat(pop(queue)).isEqualTo(s);
            assertThat(queue.bytes()).isEqualTo(0);
            assertThat(queue.count()).isEqualTo(0);
            assertThat(queue.remainingBytes()).isEqualTo(25);
            assertThat(queue.remainingCount()).isEqualTo(2);
        }
    }

    @Test
    public void testClear() throws Exception {
        ArrayRawQueue queue = new ArrayRawQueue(18 * 20, true);
        for (int i = 0; i < 20; i++) {
            String s = "test" + String.format("%02x", i);
            assertThat(push(queue, s)).isTrue();
        }
        assertThat(queue.count()).isEqualTo(20);
        assertThat(queue.bytes()).isEqualTo(360);
        assertThat(queue.remainingBytes()).isEqualTo(0);
        assertThat(queue.remainingCount()).isEqualTo(0);

        queue.clear();
        assertThat(queue.count()).isEqualTo(0);
        assertThat(queue.bytes()).isEqualTo(0);
        assertThat(queue.remainingBytes()).isEqualTo(360);
        assertThat(queue.remainingCount()).isEqualTo(30);
    }

    @Test
    public void testPushManyThenPopMany() throws Exception {
        ArrayRawQueue queue = new ArrayRawQueue(360, true);

        for (int i = 0; i < 20; i++) {
            String s = "test" + String.format("%02x", i);
            assertThat(push(queue, s)).isTrue();
        }
        for (int i = 0; i < 20; i++) {
            String s = "test" + String.format("%02x", i);
            assertThat(pop(queue)).isEqualTo(s);
        }
    }

    @Test
    public void testPushOverflow() throws Exception {
        ArrayRawQueue queue = new ArrayRawQueue(26 * 5, true);
        for (int i = 0; i < 5; i++) {
            assertThat(push(queue, "01234567890123")).isTrue();
        }
        assertThat(queue.count()).isEqualTo(5);

        assertThat(push(queue, "x")).isTrue();
        assertThat(push(queue, "y")).isTrue();
        assertThat(push(queue, "z")).isTrue();
        assertThat(queue.count()).isEqualTo(6);


        for (int i = 0; i < 3; i++)
            assertThat(pop(queue)).isEqualTo("01234567890123");

        assertThat(pop(queue)).isEqualTo("x");
        assertThat(pop(queue)).isEqualTo("y");
        assertThat(pop(queue)).isEqualTo("z");
        assertThat(pop(queue)).isEqualTo(null);
    }

    @Test
    public void testReplaceEverythingOnOverflow() throws Exception {
        ArrayRawQueue queue = new ArrayRawQueue(18 * 5, true);
        for (int i = 0; i < 5; i++) {
            assertThat(push(queue, "012345")).isTrue();
        }
        assertThat(queue.count()).isEqualTo(5);
        assertThat(push(queue, Strings.repeat("a", 18 * 5 - 11))).isFalse();
        assertThat(queue.count()).isEqualTo(5);
        assertThat(push(queue, Strings.repeat("a", 18 * 5 - 12))).isTrue();
        assertThat(queue.count()).isEqualTo(1);

        assertThat(pop(queue)).isEqualTo(Strings.repeat("a", 18 * 5 - 12));
        assertThat(pop(queue)).isEqualTo(null);
    }

    private boolean push(ArrayRawQueue queue, String s) throws IOException {
        return push(queue, s, 0);
    }

    private boolean push(ArrayRawQueue queue, String s, long timestamp) throws IOException {
        Buffer buffer = new Buffer(s.getBytes());
        buffer.setTimestamp(timestamp);
        return queue.push(buffer);
    }

    private String pop(ArrayRawQueue queue) throws IOException {
        Buffer buffer = new Buffer();
        if (!queue.pop(buffer)) return null;
        return new String(buffer.buf(), 0, buffer.count());
    }

    private String peek(ArrayRawQueue queue) throws IOException {
        Buffer buffer = new Buffer();
        if (!queue.peek(buffer)) return null;
        return new String(buffer.buf(), 0, buffer.count());
    }
}