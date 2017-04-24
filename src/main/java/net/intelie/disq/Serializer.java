package net.intelie.disq;

import java.io.InputStream;
import java.io.OutputStream;

public interface Serializer<T> {
    void serialize(OutputStream stream, T obj);

    T deserialize(InputStream stream);
}
