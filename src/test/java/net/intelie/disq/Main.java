package net.intelie.disq;

import net.intelie.disq.dson.DsonSerializer;
import net.intelie.introspective.ThreadResources;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {
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
                    Collections.singletonMap("fff", new Error("(╯°□°)╯︵ ┻━┻"))
            ));

            long start = System.nanoTime();
            long memStart = ThreadResources.allocatedBytes(Thread.currentThread());

            for (int i = 0; i < 2000000; i++)
                q.push(map);
            int count = 0, total = 0;
            while (q.count() > 0) {
                Map s = (Map) q.pop();
                total += s.size();
                count++;
            }
            System.out.println(count + " " + total + " " + count * 1e9 / (System.nanoTime() - start) + " " +
                    (ThreadResources.allocatedBytes(Thread.currentThread()) - memStart));
        }
    }

}
