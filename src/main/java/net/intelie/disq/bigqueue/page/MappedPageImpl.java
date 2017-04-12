package net.intelie.disq.bigqueue.page;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class MappedPageImpl implements IMappedPage, Closeable {
	
	private final static Logger logger = LoggerFactory.getLogger(MappedPageImpl.class);
	
	private ThreadLocalByteBuffer threadLocalBuffer;
	private volatile boolean dirty = false;
	private volatile boolean closed = false;
	private String pageFile;
	private long index;
	
	public MappedPageImpl(MappedByteBuffer mbb, String pageFile, long index) {
		this.threadLocalBuffer = new ThreadLocalByteBuffer(mbb);
		this.pageFile = pageFile;
		this.index = index;
	}
	
	public void close() throws IOException {
		synchronized(this) {
			if (closed) return;

			flush();
			
			MappedByteBuffer srcBuf = (MappedByteBuffer)threadLocalBuffer.getSourceBuffer();
			unmap(srcBuf);
			
			this.threadLocalBuffer = null; // hint GC
			
			closed = true;
			if (logger.isDebugEnabled()) {
				logger.debug("Mapped page for " + this.pageFile + " was just unmapped and closed.");
			}
		}
	}
	
	@Override
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}
	
	@Override
	public void flush() {
		synchronized(this) {
			if (closed) return;
			if (dirty) {
				MappedByteBuffer srcBuf = (MappedByteBuffer)threadLocalBuffer.getSourceBuffer();
				srcBuf.force(); // flush the changes
				dirty = false;
				if (logger.isDebugEnabled()) {
					logger.debug("Mapped page for " + this.pageFile + " was just flushed.");
				}
			}
		}
	}

	public byte[] getLocal(int position, int length) {
		ByteBuffer buf = this.getLocal(position);
		byte[] data = new byte[length];
		buf.get(data);
		return data;
	}
	
	@Override
	public ByteBuffer getLocal(int position) {
		ByteBuffer buf = this.threadLocalBuffer.get();
		buf.position(position);
		return buf;
	}
	
	private static void unmap(MappedByteBuffer buffer)
	{
		Cleaner.clean(buffer);
	}
	
    /**
     * Helper class allowing to clean direct buffers.
     */
    private static class Cleaner {
        public static final boolean CLEAN_SUPPORTED;
        private static final Method directBufferCleaner;
        private static final Method directBufferCleanerClean;

        static {
            Method directBufferCleanerX = null;
            Method directBufferCleanerCleanX = null;
            boolean v;
            try {
                directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
                directBufferCleanerX.setAccessible(true);
                directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
                directBufferCleanerCleanX.setAccessible(true);
                v = true;
            } catch (Exception e) {
                v = false;
            }
            CLEAN_SUPPORTED = v;
            directBufferCleaner = directBufferCleanerX;
            directBufferCleanerClean = directBufferCleanerCleanX;
        }

        public static void clean(ByteBuffer buffer) {
    		if (buffer == null) return;
            if (CLEAN_SUPPORTED && buffer.isDirect()) {
                try {
                    Object cleaner = directBufferCleaner.invoke(buffer);
                    directBufferCleanerClean.invoke(cleaner);
                } catch (Exception e) {
                    // silently ignore exception
                }
            }
        }
    }
    
    private static class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
    	private ByteBuffer _src;
    	
    	public ThreadLocalByteBuffer(ByteBuffer src) {
    		_src = src;
    	}
    	
    	public ByteBuffer getSourceBuffer() {
    		return _src;
    	}
    	
    	@Override
    	protected synchronized ByteBuffer initialValue() {
    		ByteBuffer dup = _src.duplicate();
    		return dup;
    	}
    }

	@Override
	public boolean isClosed() {
		return closed;
	}
	
	public String toString() {
		return "Mapped page for " + this.pageFile + ", index = " + this.index + ".";
	}

	@Override
	public String getPageFile() {
		return this.pageFile;
	}

	@Override
	public long getPageIndex() {
		return this.index;
	}
}
