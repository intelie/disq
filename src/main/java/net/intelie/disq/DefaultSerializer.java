package net.intelie.disq;

import java.io.*;
import java.util.zip.*;

public class DefaultSerializer<T> implements Serializer<T> {
    private final boolean compress;

    public DefaultSerializer() {
        this(false);
    }

    public DefaultSerializer(boolean compress) {
        this.compress = compress;
    }

    @Override
    public void serialize(OutputStream stream, T obj) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(maybeCompress(stream))) {
            oos.writeObject(obj);
            oos.flush();
        }
    }

    @Override
    public T deserialize(InputStream stream) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(maybeCompress(stream))) {
            try {
                return (T) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }

    private OutputStream maybeCompress(OutputStream stream) throws IOException {
        if (compress) stream = new DeflaterOutputStream(stream);
        return stream;
    }

    private InputStream maybeCompress(InputStream stream) throws IOException {
        if (compress) stream = new InflaterInputStream(stream);
        return stream;
    }
}
