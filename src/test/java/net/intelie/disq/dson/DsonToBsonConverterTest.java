package net.intelie.disq.dson;

import net.intelie.disq.Buffer;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.*;
import org.bson.io.BasicOutputBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

public class DsonToBsonConverterTest {
    private final MapCodec codec = new MapCodec(fromProviders(asList(
            new ValueCodecProvider(),
            new IterableCodecProvider(),
            new MapCodecProvider())));

    @Test
    public void testEmptyStream() {
        Buffer in = new Buffer();
        Buffer out = new Buffer();

        DsonToBsonConverter converter = new DsonToBsonConverter();
        assertThatThrownBy(() -> converter.convert(in.read(), out.write()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("(value) unknown DSON type");
    }

    @Test
    public void testCorruptStream() {
        Buffer in = new Buffer();
        in.write().write(45);
        Buffer out = new Buffer();

        DsonToBsonConverter converter = new DsonToBsonConverter();
        assertThatThrownBy(() -> converter.convert(in.read(), out.write()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("(value) unknown DSON type");
    }

    @Test
    public void testCorruptKey() {
        Buffer in = new Buffer();
        Buffer.OutStream inWrite = in.write();
        DsonBinaryWrite.writeType(inWrite, DsonType.OBJECT);
        DsonBinaryWrite.writeInt32(inWrite, 1);
        inWrite.write(42);

        Buffer out = new Buffer();

        DsonToBsonConverter converter = new DsonToBsonConverter();
        assertThatThrownBy(() -> converter.convert(in.read(), out.write()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("(key) unknown DSON type");
    }

    @Test
    public void testSimpleNonMapObject() {
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("value", 123.0);

        assertConversion(123.0, expected, makeControl(expected));
    }

    @Test
    public void testSimple() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("aaa", 123.0);
        map.put("bbb", true);

        assertConversion(map, map, makeControl(map));
    }

    @Test
    public void testComplexInsideObject() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("aaa", Collections.singletonMap("bbb", 123.0));
        map.put("ccc", Arrays.asList("ddd", false));

        assertConversion(map, map, makeControl(map));
    }

    @Test
    public void testNotSoSimple() {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(111, "aaa");
        map.put("111.0", "bbb");
        map.put("âçãó", true);
        map.put("ccc", null);
        map.put(Arrays.asList("ddd", "eee"), Arrays.asList(
                Collections.singletonMap(222.0, false),
                Collections.singletonMap("fff", new Error("(╯°□°)╯︵ ┻━┻"))
        ));

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("111.0", "bbb");
        expected.put("âçãó", true);
        expected.put("ccc", null);
        expected.put("dddeee", Arrays.asList(
                Collections.singletonMap("222.0", false),
                Collections.singletonMap("fff", "java.lang.Error: (╯°□°)╯︵ ┻━┻")
        ));

        assertConversion(map, expected, null);
    }

    @Test
    public void testComplexKey() {
        Map<Object, Object> key = new LinkedHashMap<>();
        key.put("aaa", 123);
        key.put(null, true);
        key.put(false, Arrays.asList(null, "(╯°□°)╯︵ ┻━┻\uD800\uDF48", 456));

        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(key, key);

        Map<String, Object> expectedKey = new LinkedHashMap<>();
        expectedKey.put("aaa", 123.0);
        expectedKey.put("", true);
        expectedKey.put("false", Arrays.asList(null, "(╯°□°)╯︵ ┻━┻\uD800\uDF48", 456.0));

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("aaa123.0truefalse(╯°□°)╯︵ ┻━┻𐍈456.0", expectedKey);

        assertConversion(map, expected, null);
    }

    private void assertConversion(Object input, Map<String, Object> expected, byte[] control) {
        Buffer in = new Buffer();
        Buffer out = new Buffer();

        DsonSerializer.Instance dson = new DsonSerializer().create();
        dson.serialize(in, input);

        DsonToBsonConverter converter = new DsonToBsonConverter();
        converter.convert(in.read(), out.write());

        if (control != null)
            assertThat(out.toArray()).isEqualTo(control);
        Map<?, ?> decoded = codec.decode(new BsonBinaryReader(ByteBuffer.wrap(out.buf(), 0, out.count())), DecoderContext.builder().build());

        assertThat(decoded).isEqualTo(expected);
    }

    private byte[] makeControl(Map<String, Object> input) {
        BasicOutputBuffer controlBuffer = new BasicOutputBuffer();
        codec.encode(new BsonBinaryWriter(controlBuffer), input, EncoderContext.builder().build());
        return controlBuffer.toByteArray();
    }
}
