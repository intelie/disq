package net.intelie.disq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Lenient {
    private static final Logger LOGGER = LoggerFactory.getLogger(Lenient.class);

    private final DiskRawQueue queue;

    public Lenient(DiskRawQueue queue) {
        this.queue = queue;
    }

    public long perform(Op supplier) throws IOException {
        try {
            queue.touch();
            return supplier.call();
        } catch (Throwable e) {
            LOGGER.info("First try queue operation error", e);
            queue.reopen();
            try {
                queue.touch();
                return supplier.call();
            } catch (Throwable e2) {
                LOGGER.info("Second try queue operation error", e2);
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
            LOGGER.info("Error closing closeable", e);
        }
    }

    public void safeDelete(Path directory) {
        try {
            Files.walkFileTree(directory, new DeleteFileVisitor());
        } catch (Throwable e) {
            LOGGER.info("Error deleting directory", e);
        }
    }

    public interface Op {
        long call() throws IOException;
    }


}
