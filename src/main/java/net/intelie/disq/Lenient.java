package net.intelie.disq;

import java.io.IOException;

public class Lenient {
    private final ByteQueue queue;

    public Lenient(ByteQueue queue) {
        this.queue = queue;
    }

    public int perform(IntOp supplier) throws IOException {
        try {
            return supplier.call();
        } catch (Exception e) {
            e.printStackTrace();
            queue.reopen();
            return supplier.call();
        }
    }

    public long perform(LongOp supplier) throws IOException {
        try {
            return supplier.call();
        } catch (Exception e) {
            e.printStackTrace();
            queue.reopen();
            return supplier.call();
        }
    }

    public boolean perform(BooleanOp supplier) throws IOException {
        try {
            return supplier.call();
        } catch (Exception e) {
            e.printStackTrace();
            queue.reopen();
            return supplier.call();
        }
    }

    public void perform(VoidOp supplier) throws IOException {
        try {
            supplier.call();
        } catch (Exception e) {
            e.printStackTrace();
            queue.reopen();
            supplier.call();
        }
    }

    public void safeClose(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            //ignoring close exception
        }
    }

    public interface IntOp {
        int call() throws IOException;
    }

    public interface LongOp {
        long call() throws IOException;
    }

    public interface BooleanOp {
        boolean call() throws IOException;
    }

    public interface VoidOp {
        void call() throws IOException;
    }


}
