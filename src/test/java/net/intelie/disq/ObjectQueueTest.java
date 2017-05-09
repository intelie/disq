package net.intelie.disq;


import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.concurrent.TimeUnit;

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

        assertThat(queue.remainingBytes()).isEqualTo(512 * 121 - 230);
        assertThat(queue.remainingCount()).isEqualTo(2683);
        assertThat(queue.count()).isEqualTo(10);
        assertThat(queue.bytes()).isEqualTo(230);
        assertThat(queue.peek()).isEqualTo("test10");

        for (int i = 10; i < 20; i++)
            assertThat(queue.pop()).isEqualTo("test" + i);

        assertThat(queue.count()).isEqualTo(0);
        assertThat(queue.bytes()).isEqualTo(230);
        assertThat(queue.remainingBytes()).isEqualTo(512 * 121 - 230);
        assertThat(queue.remainingCount()).isEqualTo(15488);
        assertThat(queue.pop()).isNull();
        assertThat(queue.peek()).isNull();
    }

    @Test
    public void testWhenTheDirectoryIsReadOnly() throws Exception {
        temp.getRoot().setWritable(false);

        DiskRawQueue bq = new DiskRawQueue(temp.getRoot().toPath(), 1000);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer(), 32, 1 << 16, false, 1000);

        queue.reopen();
        for (int i = 0; i < 20; i++)
            assertThat(queue.push("test" + i)).isTrue();
        for (int i = 0; i < 20; i++)
            assertThat(queue.pop()).isEqualTo("test" + i);
        assertThat(queue.pop()).isEqualTo(null);
        assertThat(queue.peek()).isEqualTo(null);
    }

    @Test(timeout = 3000)
    public void testBlockingWrite() throws Throwable {
        DiskRawQueue bq = new DiskRawQueue(temp.getRoot().toPath(), 1000, true, true, false);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer(), 32, 1 << 16, false);

        String s = Strings.repeat("a", 508);

        WriterThread t1 = new WriterThread(queue, s);
        t1.start();
        while (t1.getState() != Thread.State.TIMED_WAITING) {
            Thread.sleep(10);
        }

        ReaderThread t2 = new ReaderThread(queue, s);
        t2.start();

        t1.waitFinish();
        t2.waitFinish();
    }

    @Test(timeout = 3000)
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
    public void testBlockingRead() throws Throwable {
        DiskRawQueue bq = new DiskRawQueue(temp.getRoot().toPath(), 1000, true, true, false);
        ObjectQueue<Object> queue = new ObjectQueue<>(bq, new GsonSerializer(), 32, 1 << 16, false);

        String s = Strings.repeat("a", 508);

        ReaderThread t2 = new ReaderThread(queue, s);
        t2.start();
        while (t2.getState() != Thread.State.TIMED_WAITING) {
            Thread.sleep(10);
        }

        WriterThread t1 = new WriterThread(queue, s);
        t1.start();

        t1.waitFinish();
        t2.waitFinish();
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


    private static abstract class ThrowableThread extends Thread {
        private Throwable t;

        public abstract void runThrowable() throws Throwable;

        @Override
        public void run() {
            try {
                runThrowable();
            } catch (Throwable throwable) {
                t = throwable;
                t.printStackTrace();
            }
        }

        public void waitFinish() throws Throwable {
            join();
            if (t != null) throw t;
        }
    }

    private static class WriterThread extends ThrowableThread {
        private final ObjectQueue<Object> queue;
        private final String s;

        public WriterThread(ObjectQueue<Object> queue, String s) {
            this.queue = queue;
            this.s = s;
        }

        @Override
        public void runThrowable() throws InterruptedException {
            for (int i = 0; i < 200; i++) {
                queue.blockingPush(s + i);
            }
        }
    }

    private static class ReaderThread extends ThrowableThread {
        private final ObjectQueue<Object> queue;
        private final String s;

        public ReaderThread(ObjectQueue<Object> queue, String s) {
            this.queue = queue;
            this.s = s;
        }

        @Override
        public void runThrowable() throws Throwable {
            for (int i = 0; i < 200; i++) {
                assertThat(queue.blockingPop()).isEqualTo(s + i);
            }
        }
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