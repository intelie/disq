package net.intelie.disq;


import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectQueueTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testPushAndCloseThenOpenAndPop() throws Exception {
        DiskRawQueue bq = new DiskRawQueue(temp.getRoot().toPath(), 1000);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer(), 32, 1 << 16, false);

        for (int i = 0; i < 20; i++)
            queue.push("test" + i);
        for (int i = 0; i < 10; i++)
            assertThat(queue.pop()).isEqualTo("test" + i);
        queue.close();
        queue.reopen();

        assertThat(queue.count()).isEqualTo(10);
        assertThat(queue.bytes()).isEqualTo(230);
        assertThat(queue.peek()).isEqualTo("test10");

        for (int i = 10; i < 20; i++)
            assertThat(queue.pop()).isEqualTo("test" + i);

        assertThat(queue.count()).isEqualTo(0);
        assertThat(queue.bytes()).isEqualTo(230);
        assertThat(queue.pop()).isNull();
        assertThat(queue.peek()).isNull();
    }

    @Test
    public void canPushBigCompressing() throws Exception {
        DiskRawQueue bq = new DiskRawQueue(temp.getRoot().toPath(), 1000000);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer(), 32, 1 << 16, true);

        queue.push(Strings.repeat("a", 10000));

        assertThat(queue.bytes()).isLessThan(10000);
    }

    @Test
    public void canClear() throws Exception {
        DiskRawQueue bq = new DiskRawQueue(temp.getRoot().toPath(), 1000);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer(), 32, 1 << 16, false);

        for (int i = 0; i < 20; i++)
            queue.push("test" + i);
        assertThat(queue.count()).isEqualTo(20);
        queue.clear();
        assertThat(queue.count()).isEqualTo(0);
    }

    @Test
    public void canAvoidFlush() throws Exception {
        DiskRawQueue bq = new DiskRawQueue(temp.getRoot().toPath(), 1000, false, false, false);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer(), 32, 1 << 16, false);

        for (int i = 0; i < 20; i++)
            queue.push("test" + i);
        assertThat(queue.count()).isEqualTo(20);
        assertThat(new File(temp.getRoot(), "state").length()).isEqualTo(0);
        queue.flush();
        assertThat(new File(temp.getRoot(), "state").length()).isEqualTo(512);
    }

    public class GsonSerializer implements Serializer<Object> {
        Gson gson = new Gson();

        @Override
        public void serialize(OutputStream stream, Object obj) throws IOException {
            try (OutputStreamWriter writer = new OutputStreamWriter(stream)) {
                gson.toJson(obj, writer);
            }
        }

        @Override
        public Object deserialize(InputStream stream) throws IOException {
            try (InputStreamReader reader = new InputStreamReader(stream)) {
                return gson.fromJson(reader, Object.class);
            }
        }
    }

}