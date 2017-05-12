package net.intelie.disq;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.internal.stubbing.answers.ThrowsException;

import java.io.Closeable;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

public class LenientTest {
    @Test
    public void onExceptionOnTheFirstTime() throws Exception {
        DiskRawQueue queue = mock(DiskRawQueue.class);
        Lenient.Op op = mock(Lenient.Op.class);

        Lenient lenient = new Lenient(queue);

        InOrder orderly = inOrder(queue, op);

        when(op.call())
                .thenAnswer(new ThrowsException(new Error("abc")))
                .thenAnswer(new Returns(42L));

        assertThat(lenient.perform(op)).isEqualTo(42);

        orderly.verify(op).call();
        orderly.verify(queue).reopen();
    }

    @Test
    public void onExceptionAlways() throws Exception {
        DiskRawQueue queue = mock(DiskRawQueue.class);
        Lenient.Op op = mock(Lenient.Op.class);

        Lenient lenient = new Lenient(queue);

        InOrder orderly = inOrder(queue, op);

        when(op.call()).thenThrow(new Error("abc"));

        assertThatThrownBy(() -> lenient.perform(op))
                .isInstanceOf(Error.class).hasMessage("abc");

        orderly.verify(op).call();
        orderly.verify(queue).reopen();
        orderly.verify(op).call();
        orderly.verify(queue).reopen();
    }

    @Test
    public void testExceptionOnClose() throws Exception {
        Closeable closeable = mock(Closeable.class);
        doThrow(new Error("abc")).when(closeable).close();

        new Lenient(null).safeClose(closeable);
        verify(closeable).close();
    }

    @Test
    public void safeDeleteOnNonExistingFile() throws Exception {
        new Lenient(null).safeDelete(Paths.get("/whatever/does/not/exist"));
    }
}