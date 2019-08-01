package net.intelie.disq.dson;

import net.intelie.disq.Buffer;
import net.intelie.disq.Serializer;
import net.intelie.disq.SerializerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DsonSerializer implements SerializerFactory<Object> {
    @Override
    public DsonSerializer.Instance create() {
        return new Instance();
    }

    public static class Instance implements Serializer<Object> {
        private final UnicodeView unicodeView = new UnicodeView();
        private final Latin1View latin1View = new Latin1View();
        private final BiConsumer<Object, Object> SERIALIZE_DOUBLE = this::serializeDouble;
        private final Consumer<Object> SERIALIZE_SINGLE = this::serializeSingle;
        private Buffer.OutStream stream;

        @Override
        public void serialize(Buffer buffer, Object obj) {
            try (Buffer.OutStream stream = this.stream = buffer.write()) {
                serialize(stream, obj);
            } finally {
                //doing that his way so I can use Map.forEach without allocations
                this.stream = null;
            }
        }

        public void serialize(Buffer.OutStream stream, Object obj) {
            if (obj instanceof Map<?, ?>) {
                DsonBinaryWrite.writeType(stream, DsonType.OBJECT);
                DsonBinaryWrite.writeInt32(stream, ((Map<?, ?>) obj).size());
                ((Map<?, ?>) obj).forEach(SERIALIZE_DOUBLE);
            } else if (obj instanceof Collection<?>) {
                DsonBinaryWrite.writeType(stream, DsonType.ARRAY);
                DsonBinaryWrite.writeInt32(stream, ((Collection<?>) obj).size());
                ((Collection<?>) obj).forEach(SERIALIZE_SINGLE);
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

        private void serializeSingle(Object v) {
            serialize(stream, v);
        }

        private void serializeDouble(Object k, Object v) {
            serialize(stream, k);
            serialize(stream, v);
        }

        @Override
        public Object deserialize(Buffer buffer) {
            try (Buffer.InStream stream = buffer.read()) {
                return deserialize(stream);
            }

        }

        private Object deserialize(Buffer.InStream stream) {
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
                    throw new IllegalStateException("unknown DSON type");
            }
        }

    }
}
