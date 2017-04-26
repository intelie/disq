package net.intelie.disq;

import java.io.IOException;

public class Lenient {
    private final ByteQueue queue;

    public Lenient(ByteQueue queue) {
        this.queue = queue;
    }

    public int perform(Op supplier) throws IOException {
        try {
            return supplier.call();
        } catch (Exception e) {
            e.printStackTrace();
            queue.reopen();
            try {
                return supplier.call();
            } catch (Exception e2) {
                queue.reopen();
                throw e2;
            }
        }
    }

    public void safeClose(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            //ignoring close exception
        }
    }

    public interface Op {
        int call() throws IOException;
    }


}
