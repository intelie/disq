package net.intelie.disq;

import java.io.*;
import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) throws IOException {
        OutputStream os = new BufferedOutputStream(new FileOutputStream("/home/juanplopes/Downloads/queue"));
        InputStream is = new BufferedInputStream(new FileInputStream("/home/juanplopes/Downloads/queue"));

        for(int i=1; i<=100; i++) {
            os.write(i);
            os.flush();

            System.out.println(is.read());
        }
//
//
//        System.out.println(is.re);
//
//
//        IndexFile file = new IndexFile(Paths.get("/home/juanplopes/Downloads/test"));
////        file.setReadFile(1);
////        file.setReadPosition(2);
////        file.setWriteFile(3);
////        file.setWritePosition(4);
////        file.setCount(5);
//
//        System.out.println(String.format("%d %d %d %d %d",
//                file.getReadFile(), file.getReadPosition(), file.getWriteFile(), file.getWritePosition(), file.getCount()));
    }
}
