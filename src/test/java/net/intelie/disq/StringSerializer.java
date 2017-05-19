package net.intelie.disq;

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;

import java.io.*;

/**
 * Created by juanplopes2 on 19/05/17.
 */
public class StringSerializer implements Serializer<String> {
    @Override
    public void serialize(OutputStream stream, String obj) throws IOException {
        stream.write(obj.getBytes());
    }

    @Override
    public String deserialize(InputStream stream) throws IOException {
        return new String(ByteStreams.toByteArray(stream));
    }
}
