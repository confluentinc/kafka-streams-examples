package io.confluent.examples.streams;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

public class ExampleTestUtils {

    public static int randomFreeLocalPort() throws IOException {
        final ServerSocket s = new ServerSocket(0);
        final int port = s.getLocalPort();
        s.close();
        return port;
    }

    public static String randomValidHost() {
        final Random r = new Random();

        if (r.nextFloat() < 0.1) {
            return "localhost";
        } else {
            return "127." + r.nextInt(10) + "." + r.nextInt(10) + "." + r.nextInt(256);
        }
    }
}
