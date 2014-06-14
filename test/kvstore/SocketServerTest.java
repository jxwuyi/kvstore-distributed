package kvstore;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.*;

import org.junit.*;

public class SocketServerTest {

    static String localhostName;
    SocketServer ss;

    @BeforeClass
    public static void findLocalhostName() {
        try {
            localhostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.err.println(e.getMessage());
            localhostName = "localhost";
        }
    }

    @Before
    public void setup() {
        ss = new SocketServer(localhostName);
    }

    @Test
    public void serverCanConnect() throws IOException {
        ss.connect();
        assertEquals(localhostName, ss.getHostname());
        assertTrue(ss.getPort() > 0 && ss.getPort() < 65536);
        // the following is done simply to close the server socket.
        ss.stop();
        ss.start();
    }


}
