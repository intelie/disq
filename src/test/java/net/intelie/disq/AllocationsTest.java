package net.intelie.disq;

import com.google.common.collect.ImmutableMap;
import net.intelie.introspective.ThreadResources;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class AllocationsTest {
    @Test
    public void testAllocationSimpleMap() throws InterruptedException {
        int count1 = 10000;
        int count2 = 1000;
        Semaphore semaphore = new Semaphore(0);
        MyFactory factory = new MyFactory();

        Disq<Object> disq = Disq.builder(x -> semaphore.release())
                .setThreadFactory(factory)
                .setSerializer(NoopSerializer::new)
                .setInitialBufferCapacity(1000)
                .setFlushOnPop(false)
                .setFlushOnPush(false)
                .setThreadCount(1)
                .build();

        Map map = ImmutableMap.of(
                "abc", ImmutableMap.of("qwe", 72),
                55, 42.0);

        AtomicLong result = new AtomicLong();
        Thread thread = factory.newThread(() -> {
            try {
                for (int i = 0; i < count1; i++) {
                    disq.submit(map);
                }
                semaphore.acquire(count1);
                System.out.println("OK");

                disq.pause();
                long start = factory.totalAllocations();
                for (int i = 0; i < count2; i++) {
                    disq.submit(map);
                }
                disq.resume();
                semaphore.acquire(count2);

                result.set(factory.totalAllocations() - start);
                List<Thread> threads = factory.threads;
                assertThat(factory.timesCount).isEqualTo(2);
                for (int i = 0; i < threads.size(); i++) {
                    System.out.println(threads.get(i).getName() + " " + (factory.times[1][i] - factory.times[0][i]));
                }
                System.out.println(((DiskRawQueue)disq.queue().rawQueue()).flushCount());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        });
        thread.start();
        thread.join();

        assertThat(result.get() / (double) count2).isLessThan(5);
        System.out.println(result.get() / (double) count2);

    }


    private static class NoopSerializer implements Serializer<Object> {
        @Override
        public void serialize(Buffer buffer, Object obj) throws IOException {

        }

        @Override
        public Object deserialize(Buffer buffer) throws IOException {
            return "";
        }
    }

    private class MyFactory implements java.util.concurrent.ThreadFactory {
        private final List<Thread> threads = new ArrayList<>();
        private long[][] times = new long[100][100];
        private int timesCount = 0;


        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            threads.add(thread);
            return thread;
        }

        public void join() throws InterruptedException {
            for (Thread thread : threads) {
                thread.join();
            }
        }

        public long totalAllocations() {
            long sum = 0;
            for (int i = 0; i < threads.size(); i++)
                sum += times[timesCount][i] = ThreadResources.allocatedBytes(threads.get(i));
            timesCount++;
            return sum;
        }
    }
}
