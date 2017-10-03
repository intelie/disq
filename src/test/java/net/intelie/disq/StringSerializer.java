package net.intelie.disq;

import java.io.IOException;

/**
 * Created by juanplopes2 on 19/05/17.
 */
public class StringSerializer implements Serializer<String> {
    @Override
    public void serialize(Buffer buffer, String obj) throws IOException {
        buffer.write().write(obj.getBytes());
    }

    @Override
    public String deserialize(Buffer buffer) throws IOException {
        return new String(buffer.buf(), 0, buffer.count());
    }
}
