package net.intelie.disq;

import java.io.InputStream;
import java.io.OutputStream;

public interface Serializer<T> {
    void serialize(T obj, OutputStream stream);

    T deserialize(InputStream stream);
}
