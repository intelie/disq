package net.intelie.disq;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import net.intelie.disq.dson.DsonBinaryRead;
import net.intelie.disq.dson.DsonSerializer;
import net.intelie.disq.dson.Latin1View;
import net.intelie.disq.dson.UnicodeView;
import net.intelie.introspective.ThreadResources;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class AllocationsTest {
    @Test
    public void testAllocationSimpleMap() throws InterruptedException {
        int warmupCount = 10000;
        int realCount = 10000;
        MyFactory readerThreads = new MyFactory();
        MyFactory writerThreads = new MyFactory();

        AtomicLong totalCount = new AtomicLong();
        AtomicLong readerBytes = new AtomicLong();
        AtomicLong writerBytes = new AtomicLong();

        try (Disq<Object> disq = Disq.builder(x -> {
        })
                .setThreadFactory(readerThreads)
                .setSerializer(() -> new NoopSerializer(totalCount))
                .setInitialBufferCapacity(1000)
                .setFlushOnPop(false)
                .setFlushOnPush(false)
                .setThreadCount(8)
                .build()) {

            Map<Object, Object> map = new LinkedHashMap<>();
            map.put("abc", Collections.singletonMap(Strings.repeat("a", 2000), 72));
            map.put(55, 42.0);

            Thread writer = writerThreads.newThread(() -> {
                try {
                    sendSome(warmupCount, disq, map);

                    long readerStart = readerThreads.totalAllocations();
                    long writerStart = writerThreads.totalAllocations();

                    sendSome(realCount, disq, map);

                    readerBytes.set(readerThreads.totalAllocations() - readerStart);
                    writerBytes.set(writerThreads.totalAllocations() - writerStart);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }

            });
            writer.start();
            writer.join();

            System.out.println("BUF. FLUSHES: " + ((DiskRawQueue) disq.queue().rawQueue()).flushCount());
            System.out.println("READER BYTES: " + readerBytes.get());
            System.out.println("WRITER BYTES: " + writerBytes.get());
            System.out.println("OBJECT COUNT: " + totalCount.get());

            assertThat(readerBytes.get() / (double) realCount).isLessThan(1);
            assertThat(writerBytes.get() / (double) realCount).isLessThan(1);
            assertThat(totalCount.get()).isEqualTo((warmupCount + realCount) * 7);
        }
    }

    private void sendSome(int count, Disq<Object> disq, Object map) throws IOException, InterruptedException {
        disq.pause();
        for (int i = 0; i < count; i++)
            disq.submit(map);
        disq.resume();

        while (disq.count() > 0)
            Thread.sleep(10);
    }


    private static class NoopSerializer implements Serializer<Object> {
        private final DsonSerializer.Instance instance = new DsonSerializer.Instance();
        private final UnicodeView unicodeView = new UnicodeView();
        private final Latin1View latin1View = new Latin1View();
        private final AtomicLong totalCount;

        public NoopSerializer(AtomicLong totalCount) {
            this.totalCount = totalCount;
        }

        @Override
        public void serialize(Buffer buffer, Object obj) throws IOException {
            instance.serialize(buffer, obj);
        }

        @Override
        public Object deserialize(Buffer buffer) throws IOException {
            try (Buffer.InStream stream = buffer.read()) {
                int count = 0;
                for (int remaining = 1; remaining > 0; remaining--, count++) {
                    switch (DsonBinaryRead.readType(stream)) {
                        case OBJECT:
                            remaining += 2 * DsonBinaryRead.readInt32(stream);
                            break;
                        case ARRAY:
                            remaining += DsonBinaryRead.readInt32(stream);
                            break;
                        case DOUBLE:
                            DsonBinaryRead.readNumber(stream);
                            break;
                        case STRING:
                            DsonBinaryRead.readUnicode(stream, unicodeView);
                            break;
                        case STRING_LATIN1:
                            DsonBinaryRead.readLatin1(stream, latin1View);
                            break;
                        case BOOLEAN:
                            DsonBinaryRead.readBoolean(stream);
                            break;
                        case NULL:
                            return null;
                        default:
                            throw new IOException("Illegal stream state: unknown type");
                    }
                }
                totalCount.addAndGet(count);
                return null;
            }
        }
    }

    private class MyFactory implements java.util.concurrent.ThreadFactory {
        private final List<Thread> threads = new ArrayList<>();

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            threads.add(thread);
            return thread;
        }

        public long totalAllocations() {
            long sum = 0;
            for (int i = 0; i < threads.size(); i++)
                sum += ThreadResources.allocatedBytes(threads.get(i));
            return sum;
        }
    }
}
