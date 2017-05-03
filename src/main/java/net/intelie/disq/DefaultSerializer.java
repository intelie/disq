package net.intelie.disq;

import java.io.*;

public class DefaultSerializer<T> implements Serializer<T> {
    @Override
    public void serialize(OutputStream stream, T obj) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
            oos.writeObject(obj);
            oos.flush();
        }
    }

    @Override
    public T deserialize(InputStream stream) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(stream)) {
            try {
                return (T) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }

}
