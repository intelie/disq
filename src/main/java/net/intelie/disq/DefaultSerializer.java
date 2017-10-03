package net.intelie.disq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class DefaultSerializer<T> implements Serializer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSerializer.class);

    @Override
    public void serialize(Buffer.OutStream stream, T obj) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
            oos.writeObject(obj);
            oos.flush();
        }
    }

    @Override
    public T deserialize(Buffer.InStream stream) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(stream)) {
            try {
                return (T) ois.readObject();
            } catch (ClassNotFoundException e) {
                LOGGER.info("Exception on default deserializer", e);
                throw new IOException(e);
            }
        }
    }

}
