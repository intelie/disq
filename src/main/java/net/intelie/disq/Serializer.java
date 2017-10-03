package net.intelie.disq;

import java.io.IOException;

public interface Serializer<T> extends SerializerFactory {
    void serialize(Buffer buffer, T obj) throws IOException;

    T deserialize(Buffer buffer) throws IOException;

    default Serializer create() {
        return this;
    }
}
