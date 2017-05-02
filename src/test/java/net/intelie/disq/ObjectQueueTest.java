package net.intelie.disq;


import com.google.gson.Gson;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectQueueTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testPushAndCloseThenOpenAndPop() throws Exception {
        ByteQueue bq = new ByteQueue(temp.getRoot().toPath(), 1000);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer());

        for (int i = 0; i < 20; i++)
            queue.push("test" + i);
        for (int i = 0; i < 10; i++)
            assertThat(queue.pop()).isEqualTo("test" + i);
        queue.close();
        queue.reopen();

        assertThat(queue.count()).isEqualTo(10);
        assertThat(queue.bytes()).isEqualTo(390);
        assertThat(queue.peek()).isEqualTo("test10");

        for (int i = 10; i < 20; i++)
            assertThat(queue.pop()).isEqualTo("test" + i);

        assertThat(queue.count()).isEqualTo(0);
        assertThat(queue.bytes()).isEqualTo(390);
        assertThat(queue.pop()).isNull();
        assertThat(queue.peek()).isNull();
    }

    @Test
    public void canClear() throws Exception {
        ByteQueue bq = new ByteQueue(temp.getRoot().toPath(), 1000);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer());

        for (int i = 0; i < 20; i++)
            queue.push("test" + i);
        assertThat(queue.count()).isEqualTo(20);
        queue.clear();
        assertThat(queue.count()).isEqualTo(0);
    }

    @Test
    public void canAvoidFlush() throws Exception {
        ByteQueue bq = new ByteQueue(temp.getRoot().toPath(), 1000, false, false, false);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer());

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
            try (OutputStreamWriter writer = new OutputStreamWriter(new DeflaterOutputStream(stream))) {
                gson.toJson(obj, writer);
            }
        }

        @Override
        public Object deserialize(InputStream stream) throws IOException {
            try (InputStreamReader reader = new InputStreamReader(new InflaterInputStream(stream))) {
                return gson.fromJson(reader, Object.class);
            }
        }
    }

}