package net.intelie.disq;

import java.io.IOException;

public interface Serializer<T> {
    void serialize(Buffer.OutStream stream, T obj) throws IOException;

    T deserialize(Buffer.InStream stream) throws IOException;
}
