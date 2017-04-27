package net.intelie.disq;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

public class Main {
    static String s = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    public static void main(String[] args) throws Exception {
        try (ByteQueue queue = new ByteQueue(Paths.get("/home/juanplopes/Downloads/queue"), 128 * 1024 * 1024)) {
            queue.clear();
        }
        allWrites(1000000);
        allReads(100000);
    }

    private static void allReads(int n) throws IOException {
        long start = System.nanoTime();

        try (ByteQueue queue = new ByteQueue(Paths.get("/home/juanplopes/Downloads/queue"), 128 * 1024 * 1024)) {
            System.out.println(queue.count() + " " + queue.bytes());

            for (int i = 0; i < n; i++) {
                if (!s.equals(pop(queue)))
                    throw new RuntimeException("abc");

            }

            System.out.println(queue.count() + " " + queue.bytes());

            System.out.println((System.nanoTime() - start) / 1.0e9);
        }
    }

    private static void allWrites(int n) throws IOException {
        long start = System.nanoTime();

        try (ByteQueue queue = new ByteQueue(Paths.get("/home/juanplopes/Downloads/queue"), 128 * 1024 * 1024)) {
            System.out.println(queue.count() + " " + queue.bytes());

            for (int i = 0; i < n; i++) {
                push(queue, s);
            }

            System.out.println(queue.count() + " " + queue.bytes());
        }
        System.out.println((System.nanoTime() - start) / 1.0e9);
    }

    private static void push(ByteQueue queue, String s) throws IOException {
        queue.push(new Buffer(s.getBytes()));
    }

    private static String pop(ByteQueue queue) throws IOException {
        int size = queue.peekNextSize();
        if (size < 0)
            throw new NoSuchElementException("The queue is empty");
        Buffer buffer = new Buffer();
        queue.pop(buffer);
        return new String(buffer.buf());
    }
}
