package net.intelie.disq;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class GsonSerializer<T> implements Serializer<T> {
    private final Gson gson = new Gson();
    private Class<T> clazz;

    public GsonSerializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    public static GsonSerializer<Object> make() {
        return new GsonSerializer<>(Object.class);
    }

    @Override
    public void serialize(Buffer buffer, T obj) throws IOException {
        try (OutputStreamWriter writer = new OutputStreamWriter(buffer.write())) {
            gson.toJson(obj, writer);
        }
    }

    @Override
    public T deserialize(Buffer buffer) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(buffer.read())) {
            return gson.fromJson(reader, clazz);
        }
    }
}
