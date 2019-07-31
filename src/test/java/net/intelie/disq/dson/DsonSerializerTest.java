package net.intelie.disq.dson;

import net.intelie.disq.Buffer;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class DsonSerializerTest {
    @Test
    public void testStateless() {
        DsonSerializer serializer = new DsonSerializer();
        assertThat(serializer.create()).isSameAs(serializer);
    }

    @Test
    public void testSerialize() throws IOException {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(111, "aaa");
        map.put("âçãó", true);
        map.put("ccc", null);
        map.put(Arrays.asList("ddd", "eee"), Arrays.asList(
                Collections.singletonMap(222.0, false),
                Collections.singletonMap("fff", new Error("(╯°□°)╯︵ ┻━┻"))
        ));

        DsonSerializer serializer = new DsonSerializer();
        Buffer buffer = new Buffer();

        serializer.serialize(buffer, map);

        Map<Object, Object> expected = new LinkedHashMap<>();
        expected.put(111.0, "aaa");
        expected.put("âçãó", true);
        expected.put("ccc", null);
        expected.put(Arrays.asList("ddd", "eee"), Arrays.asList(
                Collections.singletonMap(222.0, false),
                Collections.singletonMap("fff", "java.lang.Error: (╯°□°)╯︵ ┻━┻")
        ));

        assertThat(serializer.deserialize(buffer)).isEqualTo(expected);
    }
}