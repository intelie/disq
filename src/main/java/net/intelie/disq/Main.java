package net.intelie.disq;

import java.io.IOException;
import java.nio.file.Paths;

public class Main {
    static String s = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    private static ObjectQueue<String> open() throws IOException {
        return new ObjectQueue<>(new ByteQueue(Paths.get("/home/juanplopes/Downloads/queue"), 1024 * 1024 * 1024), new JavaSerializer<>());
    }

    public static void main(String[] args) throws Exception {
        try (ObjectQueue<String> queue = open()) {
            queue.clear();
        }
        allWrites(1000000);
        allReads(1000000);
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
