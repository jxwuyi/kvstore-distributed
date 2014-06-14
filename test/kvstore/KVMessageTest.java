package kvstore;

import static kvstore.KVConstants.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.*;
import java.net.*;
import java.util.*;

import org.junit.*;
public class KVMessageTest {

    private Socket sock;

    private static final String TEST_INPUT_DIR = "test/kvstore/test-inputs/";

    @Test
    public void successfullyParsesPutReq() throws KVException {
        setupSocket("putreq.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(PUT_REQ, kvm.getMsgType());
        assertNull(kvm.getMessage());
        assertNotNull(kvm.getKey());
        assertNotNull(kvm.getValue());
    }

    @Test
    public void successfullyParsesPutResp() throws KVException {
        setupSocket("putresp.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(RESP, kvm.getMsgType());
        assertTrue(SUCCESS.equalsIgnoreCase(kvm.getMessage()));
        assertNull(kvm.getKey());
        assertNull(kvm.getValue());
    }
    
    @Test
    public void successfullyParsesGetReq() throws KVException {
        setupSocket("getreq.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(GET_REQ, kvm.getMsgType());
        assertNull(kvm.getMessage());
        assertNotNull(kvm.getKey());
        assertNull(kvm.getValue());
    }

    @Test
    public void successfullyParsesGetResp() throws KVException {
        setupSocket("getresp.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(RESP, kvm.getMsgType());
        assertNull(kvm.getMessage());
        assertNotNull(kvm.getKey());
        assertNotNull(kvm.getValue());
    }
    
    @Test
    public void successfullyParsesDelReq() throws KVException {
        setupSocket("delreq.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(DEL_REQ, kvm.getMsgType());
        assertNull(kvm.getMessage());
        assertNotNull(kvm.getKey());
        assertNull(kvm.getValue());
    }

    @Test
    public void successfullyParsesDelResp() throws KVException {
        setupSocket("delresp.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(RESP, kvm.getMsgType());
        assertTrue(SUCCESS.equalsIgnoreCase(kvm.getMessage()));
        assertNull(kvm.getKey());
        assertNull(kvm.getValue());
    }
    
    @Test
    public void successfullyParsesErrorResp() throws KVException {
        setupSocket("errorresp.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(RESP, kvm.getMsgType());
        assertTrue(!SUCCESS.equalsIgnoreCase(kvm.getMessage()));
        assertNull(kvm.getKey());
        assertNull(kvm.getValue());
    }
    
    @Test
    public void ParsesDelReqWithoutPrologue() throws KVException {
        setupSocket("no-prologue.txt");
        // TODO: to make sure what the correct behavior is in this case
        try {
        	KVMessage kvm = new KVMessage(sock);
        	assertNotNull(kvm);
            assertEquals(DEL_REQ, kvm.getMsgType());
            assertNull(kvm.getMessage());
            assertNotNull(kvm.getKey());
            assertNull(kvm.getValue());
        } catch (Exception e) {
        	fail("Should not fail on XML file without prologue!");
        }
    }
    
    @Test
    public void ParsesGarbage() throws KVException {
        setupSocket("garbage.txt");
        try {
        	new KVMessage(sock);
            fail("Didn't fail on Garbage file!");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_PARSER);
        }
    }
    
    @Test
    public void ParsesInvalidPutReq() throws KVException {
        setupSocket("invalid-putreq.txt");
        try {
        	new KVMessage(sock);
            fail("Didn't fail on invalid put request XML file!");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        }
    }
    
    @Test
    public void ParsesInvalidPutResp() throws KVException {
        setupSocket("invalid-putresp.txt");
        try {
        	new KVMessage(sock);
            fail("Didn't fail on invalid put response XML file!");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        }
    }
    
    @Test
    public void ParsesInvalidGetReq() throws KVException {
        setupSocket("invalid-getreq.txt");
        try {
        	new KVMessage(sock);
            fail("Didn't fail on invalid get request XML file!");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        }
    }
    
    @Test
    public void ParsesInvalidGetResp() throws KVException {
        setupSocket("invalid-getresp.txt");
        try {
        	new KVMessage(sock);
            fail("Didn't fail on invalid get response XML file!");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_FORMAT);
        }
    }

    /* Begin helper methods */

    private void setupSocket(String filename) {
        sock = mock(Socket.class);
        File f = new File(System.getProperty("user.dir"), TEST_INPUT_DIR + filename);
        try {
            doNothing().when(sock).setSoTimeout(anyInt());
            when(sock.getInputStream()).thenReturn(new FileInputStream(f));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
