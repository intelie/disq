package net.intelie.disq;

import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;

import java.io.*;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

public class DisqTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testTempPersistentQueue() throws Exception {
        Processor<Object> processor = mock(Processor.class);
        Path saved = null;
        try (Disq<Object> disq = Disq.builder(processor).build(true)) {
            assertThat(disq.queue().fallbackQueue().remainingBytes()).isEqualTo(0);

            disq.submit("test1");
            disq.submit("test2");
            disq.submit("test3");

            assertThat(disq.count()).isEqualTo(3);
            verifyZeroInteractions(processor);

            disq.resume();

            while (disq.count() > 0)
                Thread.sleep(10);

            saved = ((DiskRawQueue) disq.queue().rawQueue()).path();
            assertThat(saved).exists();
        }
        InOrder ordered = inOrder(processor);
        ordered.verify(processor).process("test1");
        ordered.verify(processor).process("test2");
        ordered.verify(processor).process("test3");
        ordered.verifyNoMoreInteractions();

        assertThat(saved).doesNotExist();
    }

    @Test
    public void testTempPersistentQueueFallbackSize() throws Exception {
        Processor<Object> processor = mock(Processor.class);
        try (Disq<Object> disq = Disq.builder(processor).setFallbackBufferCapacity(1024).build(true)) {
            assertThat(disq.queue().fallbackQueue().remainingBytes()).isEqualTo(1024);
        }
    }

    @Test
    public void testSpecificPathLimitedQueue() throws Exception {
        Processor<String> processor = mock(Processor.class);
        String s = Strings.repeat("a", (int) StateFile.MIN_QUEUE_SIZE / 2 - 5);

        Path saved = null;
        try (Disq<String> disq = Disq.builder(processor)
                .setMaxSize(StateFile.MIN_QUEUE_SIZE)
                .setSerializer(new StringSerializer())
                .setDirectory(temp.getRoot().toPath())
                .build()) {
            disq.pause();


            disq.submit(s + "1");
            disq.submit(s + "2");
            disq.submit(s + "3");

            assertThat(disq.bytes()).isEqualTo(StateFile.MIN_QUEUE_SIZE);
            assertThat(disq.remainingBytes()).isEqualTo(0);
            assertThat(disq.count()).isEqualTo(2);
            verifyZeroInteractions(processor);

            disq.resume();

            while (disq.count() > 0)
                Thread.sleep(10);

            saved = ((DiskRawQueue) disq.queue().rawQueue()).path();
            assertThat(saved).exists();
        }

        InOrder ordered = inOrder(processor);
        ordered.verify(processor).process(s + "1");
        ordered.verify(processor).process(s + "2");
        ordered.verifyNoMoreInteractions();


        assertThat(saved).exists();
    }

    @Test
    public void testSpecificPathCompressedueueReopening() throws Exception {
        String s = Strings.repeat("a", (int) StateFile.MIN_QUEUE_SIZE / 2 - 5);

        Processor<String> processor = mock(Processor.class);
        doThrow(new Error()).when(processor).process(anyString());

        Path saved = null;
        try (Disq<String> disq = Disq.builder(processor)
                .setMaxSize(StateFile.MIN_QUEUE_SIZE)
                .setCompress(true)
                .setSerializer(new StringSerializer())
                .setDirectory(temp.getRoot().getAbsolutePath())
                .setFlushOnPop(false)
                .setFlushOnPush(false)
                .setDeleteOldestOnOverflow(true)
                .build()) {
            disq.pause();
            disq.submit(s + "1");
            disq.submit(s + "2");
            disq.submit(s + "3");

            assertThat(disq.bytes()).isEqualTo(177);
            assertThat(disq.remainingBytes()).isEqualTo(StateFile.MIN_QUEUE_SIZE - disq.bytes());
            assertThat(disq.count()).isEqualTo(3);
            verifyZeroInteractions(processor);
        }

        try (Disq<String> disq = Disq.builder(processor)
                .setCompress(true)
                .setSerializer(new StringSerializer())
                .setDirectory(temp.getRoot().toPath())
                .build()) {
            while (disq.count() > 0)
                Thread.sleep(10);

            saved = ((DiskRawQueue) disq.queue().rawQueue()).path();
            assertThat(saved).exists();
        }

        InOrder ordered = inOrder(processor);
        ordered.verify(processor).process(s + "1");
        ordered.verify(processor).process(s + "2");
        ordered.verify(processor).process(s + "3");
        ordered.verifyNoMoreInteractions();

        assertThat(saved).exists();
    }

    @Test
    public void testMaxBufferSize() throws Exception {
        String s = Strings.repeat("a", 1001);

        PersistentQueue<Object> queue = Disq.builder()
                .setInitialBufferCapacity(100)
                .setMaxBufferCapacity(1000)
                .buildPersistentQueue();

        assertThatThrownBy(() -> queue.push(s))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Buffer overflowed")
                .hasMessageContaining("1008/1000");
    }

    @Test
    public void testThreadNames() throws Exception {
        try (Disq<String> disq = Disq.<String>builder().setNamedThreadFactory("abcdef-%d").setThreadCount(4).build()) {
            List<String> threadList = Thread.getAllStackTraces().keySet().stream().map(Thread::getName).collect(Collectors.toList());

            assertThat(threadList).contains("abcdef-0", "abcdef-1", "abcdef-2", "abcdef-3");
            assertThat(threadList).doesNotContain("abcdef-4");
        }
    }

    private static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(OutputStream stream, String obj) throws IOException {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream));
            writer.write(obj);
            writer.flush();
        }

        @Override
        public String deserialize(InputStream stream) throws IOException {
            return CharStreams.toString(new InputStreamReader(stream));
        }
    }
}