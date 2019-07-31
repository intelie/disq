package net.intelie.disq.dson;

import net.intelie.disq.Buffer;
import net.intelie.disq.Serializer;
import net.intelie.disq.SerializerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class DsonSerializer implements SerializerFactory<Object> {
    @Override
    public DsonSerializer.Instance create() {
        return new Instance();
    }

    public class Instance implements Serializer<Object> {
        private UnicodeView unicodeView = new UnicodeView();
        private Latin1View latin1View = new Latin1View();

        @Override
        public void serialize(Buffer buffer, Object obj) throws IOException {
            try (Buffer.OutStream stream = buffer.write()) {
                serialize(stream, obj);
            }
        }

        public void serialize(Buffer.OutStream stream, Object obj) throws IOException {
            if (obj instanceof Map<?, ?>) {
                DsonBinaryWrite.writeType(stream, DsonType.OBJECT);
                DsonBinaryWrite.writeInt32(stream, ((Map<?, ?>) obj).size());
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) obj).entrySet()) {
                    serialize(stream, entry.getKey());
                    serialize(stream, entry.getValue());
                }
            } else if (obj instanceof Collection<?>) {
                DsonBinaryWrite.writeType(stream, DsonType.ARRAY);
                DsonBinaryWrite.writeInt32(stream, ((Collection<?>) obj).size());
                for (Object child : (Iterable<?>) obj)
                    serialize(stream, child);
            } else if (obj instanceof CharSequence) {
                CharSequence str = (CharSequence) obj;
                boolean latin1 = true;
                for (int i = 0; latin1 && i < str.length(); i++) {
                    latin1 = str.charAt(i) < 256;
                }
                if (latin1) {
                    DsonBinaryWrite.writeType(stream, DsonType.STRING_LATIN1);
                    DsonBinaryWrite.writeLatin1(stream, str);
                } else {
                    DsonBinaryWrite.writeType(stream, DsonType.STRING);
                    DsonBinaryWrite.writeUnicode(stream, str);
                }
            } else if (obj instanceof Number) {
                DsonBinaryWrite.writeType(stream, DsonType.DOUBLE);
                DsonBinaryWrite.writeNumber(stream, ((Number) obj).doubleValue());
            } else if (obj instanceof Boolean) {
                DsonBinaryWrite.writeType(stream, DsonType.BOOLEAN);
                DsonBinaryWrite.writeBoolean(stream, (Boolean) obj);
            } else if (obj == null) {
                DsonBinaryWrite.writeType(stream, DsonType.NULL);
            } else {
                serialize(stream, obj.toString());
            }
        }

        @Override
        public Object deserialize(Buffer buffer) throws IOException {
            try (Buffer.InStream stream = buffer.read()) {
                return deserialize(stream);
            }

        }

        private Object deserialize(Buffer.InStream stream) throws IOException {
            switch (DsonBinaryRead.readType(stream)) {
                case OBJECT:
                    int objectSize = DsonBinaryRead.readInt32(stream);
                    Map<Object, Object> map = new LinkedHashMap<>(objectSize);
                    for (int i = 0; i < objectSize; i++) {
                        Object key = deserialize(stream);
                        Object value = deserialize(stream);
                        map.put(key, value);
                    }
                    return map;
                case ARRAY:
                    int arraySize = DsonBinaryRead.readInt32(stream);
                    ArrayList<Object> list = new ArrayList<>(arraySize);
                    for (int i = 0; i < arraySize; i++)
                        list.add(deserialize(stream));
                    return list;
                case DOUBLE:
                    return DsonBinaryRead.readNumber(stream);
                case STRING:
                    DsonBinaryRead.readUnicode(stream, unicodeView);
                    return unicodeView.toString();
                case STRING_LATIN1:
                    DsonBinaryRead.readLatin1(stream, latin1View);
                    return latin1View.toString();
                case BOOLEAN:
                    return DsonBinaryRead.readBoolean(stream);
                case NULL:
                    return null;
                default:
                    throw new IOException("Illegal stream state: unknown type");
            }
        }

    }
}
