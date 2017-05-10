package net.intelie.disq;

import com.google.common.base.Strings;

import java.io.IOException;
import java.nio.file.Paths;

public class Main {
    static String s = Strings.repeat("a", 1000);

    private static ObjectQueue<String> open() throws IOException {
        DiskRawQueue bq = new DiskRawQueue(Paths.get("/home/juanplopes/Downloads/queue"), 10L * 1024 * 1024 * 1024, true, true, false);
        return new ObjectQueue<>(bq, new DefaultSerializer<>(), 16000, -1, false);
    }

    public static void main(String[] args) throws Exception {
        try (Disq<Object> queue = Disq
                .builder(x -> {
                    //System.out.println(Thread.currentThread().getId() + " " + x);
                })
                .build()) {

            String s = Strings.repeat("a", 2000);
            for (int i = 0; i < 100000; i++) {
                queue.submit(s);
            }
            //Thread.sleep(100000);
            System.out.println("OAAA");
            queue.resume();
        }


        /*try (ObjectQueue<String> queue = open()) {
            queue.clear();
        }
        allWrites(100000);
        allReads(100000);*/
    }

    private static void allReads(int n) throws IOException {
        long start = System.nanoTime();

        try (ObjectQueue<String> queue = open()) {
            System.out.println(queue.count() + " " + queue.bytes());

            for (int i = 0; i < n; i++) {
                String q = queue.pop();
                if (!s.equals(q))
                    throw new RuntimeException("abc: " + q);

            }

            System.out.println(queue.count() + " " + queue.bytes());

            System.out.println((System.nanoTime() - start) / 1.0e9);
        }
    }

    private static void allWrites(int n) throws IOException {
        long start = System.nanoTime();

        try (ObjectQueue<String> queue = open()) {
            System.out.println(queue.count() + " " + queue.bytes());

            for (int i = 0; i < n; i++) {
                queue.push(s);
            }

            System.out.println(queue.count() + " " + queue.bytes());
        }
        System.out.println((System.nanoTime() - start) / 1.0e9);
    }

}
