package net.intelie.disq;

import java.io.IOException;
import java.nio.file.Paths;

public class Main {
    static String s = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    private static ObjectQueue<String> open() throws IOException {
        ByteQueue bq = new ByteQueue(Paths.get("/home/juanplopes/Downloads/queue"), 1024 * 1024 * 1024, true, true, false);
        return new ObjectQueue<>(bq, new DefaultSerializer<>(false));
    }

    public static void main(String[] args) throws Exception {
        try (ObjectQueue<String> queue = open()) {
            queue.clear();
        }
        allWrites(100000);
        allReads(100000);
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
