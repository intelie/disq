package net.intelie.disq;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Serializer<T> {
    void serialize(OutputStream stream, T obj) throws IOException;

    T deserialize(InputStream stream) throws IOException;
}
