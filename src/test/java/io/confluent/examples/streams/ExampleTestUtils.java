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
}
