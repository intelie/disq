package net.intelie.disq;

import com.google.gson.internal.LinkedHashTreeMap;
import com.google.gson.internal.LinkedTreeMap;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.serializers.FSTMapSerializer;

import java.io.IOException;
import java.util.LinkedHashMap;

public class FstSerializer implements SerializerFactory<Object> {
    @Override
    public Serializer<Object> create() {
        return new Serializer<Object>() {
            private final FSTConfiguration conf = newFSTConfiguration();
            private final FSTObjectOutput output = new FSTObjectOutput(conf);
            private final FSTObjectInput input = new FSTObjectInput(conf);

            @Override
            public void serialize(Buffer buffer, Object obj) throws IOException {
                try (Buffer.OutStream stream = buffer.write()) {
                    output.resetForReUse(stream);

                    output.writeObject(obj);
                    output.flush();

                    output.resetForReUse();
                }
            }

            @Override
            public Object deserialize(Buffer buffer) throws IOException {
                input.resetForReuseUseArray(buffer.buf(), buffer.count());
                try {
                    return input.readObject();
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }

            }
        };
    }

    private static FSTConfiguration newFSTConfiguration() {
        FSTConfiguration fstConfiguration = FSTConfiguration.createDefaultConfiguration();
        fstConfiguration.setShareReferences(false);

        // Workaround for https://github.com/RuedigerMoeller/fast-serialization/issues/204
        fstConfiguration.registerSerializer(LinkedTreeMap.class, new FSTMapSerializer(), true);
        fstConfiguration.registerSerializer(LinkedHashTreeMap.class, new FSTMapSerializer(), true);
        fstConfiguration.registerClass(LinkedTreeMap.class, LinkedHashTreeMap.class);

        return fstConfiguration;
    }
}
