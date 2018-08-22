package io.confluent.examples.streams;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

public class ExampleTestUtils {

    public static int randomFreeLocalPort() throws IOException {
        ServerSocket s = new ServerSocket(0);
        int port = s.getLocalPort();
        s.close();
        return port;
    }

    public static String randomHost() {
        Random r = new Random();
        return r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
    }
}
