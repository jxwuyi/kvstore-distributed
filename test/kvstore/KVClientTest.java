package kvstore;

import static kvstore.KVConstants.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.*;
import java.net.*;

import org.junit.*;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KVClient.class)
public class KVClientTest {

    KVClient client;
    Socket sock;

    private static final String TEST_INPUT_DIR = "test/kvstore/test-inputs/";
    private static File tempFile;

    @BeforeClass
    public static void setupTempFile() throws IOException {
        tempFile = File.createTempFile("TestKVClient-", ".txt");
        tempFile.deleteOnExit();
    }

    @Before
    public void setupClient() throws IOException {
        String hostname = InetAddress.getLocalHost().getHostAddress();
        client = new KVClient(hostname, 8080);
    }

    @Test(timeout = 20000)
    public void testInvalidKey() {
        try {
            client.put("", "bar");
            fail("Didn't fail on empty key");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_KEY);
        }
    }
    
    @Test(timeout = 20000)
    public void testInvalidValue() {
        try {
            client.put("foo", null);
            fail("Didn't fail on empty value");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_VALUE);
        }
    }


    private void setupSocket(String filename) throws Exception {
        sock = mock(Socket.class);
        whenNew(Socket.class).withArguments(anyString(), anyInt()).thenReturn(sock);
        File f = new File(System.getProperty("user.dir"), TEST_INPUT_DIR + filename);
        doNothing().when(sock).setSoTimeout(anyInt());
        when(sock.getOutputStream()).thenReturn(new FileOutputStream(tempFile));
        when(sock.getInputStream()).thenReturn(new FileInputStream(f));
    }

}
