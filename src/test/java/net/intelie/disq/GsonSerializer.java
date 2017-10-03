package net.intelie.disq;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * Created by juanplopes2 on 19/05/17.
 */
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
    public void serialize(Buffer.OutStream stream, T obj) throws IOException {
        try (OutputStreamWriter writer = new OutputStreamWriter(stream)) {
            gson.toJson(obj, writer);
        }
    }

    @Override
    public T deserialize(Buffer.InStream stream) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(stream)) {
            return gson.fromJson(reader, clazz);
        }
    }
}
