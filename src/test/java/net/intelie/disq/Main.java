package net.intelie.disq;

import com.google.common.base.Strings;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Main {
    static String s = Strings.repeat("a", 1000);

    private static PersistentQueue<String> open() throws IOException {
        DiskRawQueue bq = new DiskRawQueue(Paths.get("/home/juanplopes/Downloads/queue"), 10L * 1024 * 1024 * 1024, false, false, false);
        return new PersistentQueue<>(bq, new StringSerializer(), 16000, -1, false);
    }

    public static void main(String[] args) throws Exception {
        PersistentQueue<Object> q = Disq.builder()
                .setCompress(true)
                .setDirectory("/home/juanplopes/Downloads/test/core.storage.main")
                .buildPersistentQueue();

        for (int i = 0; i < 100; i++)
            q.push("hi");
        while (q.count() > 0) {
            try {
                System.out.println(q.pop());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        //test();

        /*try (PersistentQueue<String> queue = open()) {
            queue.clear();
        }
        while(true) {
            allWrites(100000);
            allWrites(100000);
            allWrites(100000);
            allReads(100000);
            allReads(100000);
            allReads(100000);
        }*/
    }

    private static void test() throws Exception {
        long start = System.nanoTime();

        Map<String, Integer> map = new HashMap<String, Integer>() {
            @Override
            public Integer get(Object key) {
                return containsKey(key) ? super.get(key) : 0;
            }
        };

        Disq<Object> queue = Disq
                .builder(x -> {
                    map.put(Thread.currentThread().getName(), map.get(Thread.currentThread().getName()) + 1);
                })
                .setDirectory("/home/juanplopes/Downloads/queue")
                .setMaxSize(1 << 28)
                .setThreadCount(8)
                .setFlushOnPop(false)
                .setFlushOnPush(false)
                .setNamedThreadFactory("%d")
                .setDeleteOldestOnOverflow(true)
                .build(true);
        queue.clear();
        queue.resume();

        String s = Strings.repeat("a", 1000);
        for (int i = 0; i < 100000; i++) {
            queue.submit(s);
        }

        //Thread.sleep(100000);
        System.out.println("OAAA " + (System.nanoTime() - start) / 1e9 + " " + queue.count());
        start = System.nanoTime();

        while (queue.count() > 0)
            Thread.sleep(100);

        System.out.println(map);
        System.out.println(map.values().stream().mapToInt(x -> x).sum());
        System.out.println("OAAA " + (System.nanoTime() - start) / 1e9);

        queue.close();

    }

    private static void allReads(int n) throws IOException {
        long start = System.nanoTime();

        try (PersistentQueue<String> queue = open()) {
            //System.out.println(queue.count() + " " + queue.bytes());

            for (int i = 0; i < n; i++) {
                String q = queue.pop();
                if (!s.equals(q))
                    throw new RuntimeException("abc: " + q);

            }

            //System.out.println(queue.count() + " " + queue.bytes());

            System.out.println("read " + (System.nanoTime() - start) / 1.0e9);
        }
    }

    private static void allWrites(int n) throws IOException {
        long start = System.nanoTime();

        try (PersistentQueue<String> queue = open()) {
            //System.out.println(queue.count() + " " + queue.bytes());

            for (int i = 0; i < n; i++) {
                queue.push(s);
            }

            //System.out.println(queue.count() + " " + queue.bytes());
        }
        System.out.println("write " + (System.nanoTime() - start) / 1.0e9);
    }

}
