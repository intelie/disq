package net.intelie.disq;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.internal.stubbing.answers.ThrowsException;

import java.io.Closeable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

public class LenientTest {
    @Test
    public void onExceptionOnTheFirstTime() throws Exception {
        ByteQueue queue = mock(ByteQueue.class);
        Lenient.Op op = mock(Lenient.Op.class);

        Lenient lenient = new Lenient(queue);

        InOrder orderly = inOrder(queue, op);

        when(op.call())
                .thenAnswer(new ThrowsException(new Error("abc")))
                .thenAnswer(new Returns(true));

        assertThat(lenient.perform(op)).isEqualTo(true);

        orderly.verify(op).call();
        orderly.verify(queue).reopen();
    }

    @Test
    public void onExceptionAlways() throws Exception {
        ByteQueue queue = mock(ByteQueue.class);
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
}