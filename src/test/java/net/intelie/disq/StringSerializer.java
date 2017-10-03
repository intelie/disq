package net.intelie.disq;

import com.google.common.io.ByteStreams;

import java.io.IOException;

/**
 * Created by juanplopes2 on 19/05/17.
 */
public class StringSerializer implements Serializer<String> {
    @Override
    public void serialize(Buffer.OutStream stream, String obj) throws IOException {
        stream.write(obj.getBytes());
    }

    @Override
    public String deserialize(Buffer.InStream stream) throws IOException {
        return new String(ByteStreams.toByteArray(stream));
    }
}
