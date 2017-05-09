package net.intelie.disq;

import java.io.IOException;

public class Lenient {
    private final DiskRawQueue queue;

    public Lenient(DiskRawQueue queue) {
        this.queue = queue;
    }

    public long perform(Op supplier) throws IOException {
        try {
            queue.touch();
            return supplier.call();
        } catch (Throwable e) {
            e.printStackTrace();
            queue.reopen();
            try {
                queue.touch();
                return supplier.call();
            } catch (Throwable e2) {
                queue.reopen();
                throw e2;
            }
        }
    }

    public long performSafe(Op supplier, long defaultValue) {
        try {
            return perform(supplier);
        } catch (Throwable e) {
            return defaultValue;
        }
    }


    public void safeClose(AutoCloseable closeable) {
        try {
            if (closeable != null)
                closeable.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public interface Op {
        long call() throws IOException;
    }


}
