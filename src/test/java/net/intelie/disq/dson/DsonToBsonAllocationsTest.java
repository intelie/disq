package net.intelie.disq.dson;

import net.intelie.disq.Buffer;
import net.intelie.disq.SuppressForbidden;
import net.intelie.introspective.ThreadResources;
import org.bson.codecs.IterableCodecProvider;
import org.bson.codecs.MapCodec;
import org.bson.codecs.MapCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

public class DsonToBsonAllocationsTest {
    private final MapCodec codec = new MapCodec(fromProviders(asList(
            new ValueCodecProvider(),
            new IterableCodecProvider(),
            new MapCodecProvider())));


    @Test
    public void testSimple() {
        int warmup = 10000, realTest = 10000;

        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("aaa", 123.0);
        map.put("bbb", true);
        map.put(123.0, Arrays.asList(123, "(╯°□°)╯︵ ┻━┻\uD800\uDF48"));

        Buffer in = new Buffer();
        Buffer out = new Buffer();

        DsonSerializer.Instance dson = new DsonSerializer().create();

        DsonToBsonConverter converter = new DsonToBsonConverter();

        for (int i = 0; i < warmup; i++) {
            dson.serialize(in, map);
            converter.convert(in.read(), out.write());
        }

        long start = ThreadResources.allocatedBytes(Thread.currentThread());
        for (int i = 0; i < realTest; i++) {
            dson.serialize(in, map);
            converter.convert(in.read(), out.write());
        }
        long result = ThreadResources.allocatedBytes(Thread.currentThread()) - start;

        assertThat(result / (double) realTest).isLessThan(1);

        printStats(result);
    }

    @SuppressForbidden
    private static void printStats(long result) {
        System.out.println("ALLOCATIONS: " + result);
    }
}
