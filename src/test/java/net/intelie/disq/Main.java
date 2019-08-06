package net.intelie.disq;

import com.google.gson.Gson;
import net.intelie.disq.dson.DsonSerializer;
import net.intelie.introspective.ThreadResources;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {
        Map map2 = new Gson().fromJson(
                "{\"index_timestamp\":1.56294378011E12,\"wellbore_name\":\"1\",\"adjusted_index_timestamp\":1.562943817363E12,\"source\":\"WITS\",\"depth_value\":6717.527,\"uom\":\"unitless\",\"extra\":\"RBNvo1WzZ4o\",\"mnemonic\":\"STKNUM\",\"well_name\":\"MP72 – A11 ST\",\"depth_mnemonic\":\"DEPTMEAS\",\"value\":0.0,\"errors\":[\"missing_src_unit\",\"unknown_src_unit\"],\"timestamp\":1.562943818361E12,\"__type\":\"ensco75\",\"__src\":\"replay/rig11_b\"}",
                Map.class);

        try (PersistentQueue<Object> q = Disq.builder()
                .setFlushOnPop(false)
                .setFlushOnPush(false)
                .setSerializer(new DsonSerializer())
                .setDirectory("/home/juanplopes/Downloads/test/core.storage.main")
                .buildPersistentQueue()) {
            q.clear();

            Map<Object, Object> map = new LinkedHashMap<>();
            map.put(111, "aaa");
            map.put("âçãó", true);
            map.put("ccc", null);
            map.put(Arrays.asList("ddd", "eee"), Arrays.asList(
                    Collections.singletonMap(222.0, false),
                    Collections.singletonMap("fff", "(╯°□°)╯︵ ┻━┻")
            ));

            long start = System.nanoTime();
            long memStart = ThreadResources.allocatedBytes(Thread.currentThread());

            for (int i = 0; i < 1000000; i++)
                q.push(map);

            double writeTime = (System.nanoTime() - start) / 1e9;

            long bytes = q.bytes();
            int count = 0, total = 0;
            while (q.count() > 0) {
                Map s = (Map) q.pop();
                total += s.size();
                count++;
            }
            double readTime = (System.nanoTime() - start) / 1e9 - writeTime;

            System.out.println(bytes + " " + total);
            System.out.println(writeTime + " " + bytes / writeTime / (double) (1 << 20));
            System.out.println(readTime + " " + bytes / readTime / (double) (1 << 20));
            System.out.println((ThreadResources.allocatedBytes(Thread.currentThread()) - memStart) / (double) count);

        }
    }

}
