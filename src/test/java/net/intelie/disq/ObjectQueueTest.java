package net.intelie.disq;


import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.not;

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

    @Test(timeout = 3000)
    public void testBlockingWrite() throws Exception {
        DiskRawQueue bq = new DiskRawQueue(temp.getRoot().toPath(), 1000, true, true, false);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer(), 32, 1 << 16, false);

        String s = Strings.repeat("a", 508);

        Thread t1 = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < 200; i++) {
                    try {
                        System.out.println("WRITE " + i);
                        queue.blockingPush(s + i);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        t1.start();
        while (t1.getState() != Thread.State.TIMED_WAITING) {
            Thread.sleep(50);
        }

        Thread t2 = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < 200; i++) {
                    try {
                        System.out.println("READ " + i);
                        assertThat(queue.blockingPop()).isEqualTo(s + i);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        t2.start();

        t1.join();
        t2.join();
    }

    @Test
    public void testBlockingTimeout() throws Exception {
        DiskRawQueue bq = new DiskRawQueue(temp.getRoot().toPath(), 1000, true, true, false);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer(), 32, 1 << 16, false);

        String s = Strings.repeat("a", 506);

        for (int i = 0; i < 121; i++)
            assertThat(queue.blockingPush(s, 10, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(queue.blockingPush(s, 10, TimeUnit.MILLISECONDS)).isFalse();

        for (int i = 0; i < 121; i++)
            assertThat(queue.blockingPop(10, TimeUnit.MILLISECONDS)).isEqualTo(s);
        assertThat(queue.blockingPop(10, TimeUnit.MILLISECONDS)).isEqualTo(null);
    }

    @Test(timeout = 3000)
    public void testBlockingRead() throws Exception {
        DiskRawQueue bq = new DiskRawQueue(temp.getRoot().toPath(), 1000, true, true, false);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer(), 32, 1 << 16, false);

        String s = Strings.repeat("a", 508);

        Thread t2 = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < 200; i++) {
                    try {
                        System.out.println("READ " + i);
                        assertThat(queue.blockingPop()).isEqualTo(s + i);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        t2.start();
        while (t2.getState() != Thread.State.TIMED_WAITING) {
            Thread.sleep(50);
        }

        Thread t1 = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < 200; i++) {
                    try {
                        System.out.println("WRITE " + i);
                        queue.blockingPush(s + i);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        t1.start();

        t1.join();
        t2.join();
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