package kvstore;

import static org.junit.Assert.*;

import org.junit.Test;

public class TPCEndToEndTest extends TPCEndToEndTemplate {

    @Test(timeout = 15000)
    public void testSinglePutGet() throws KVException {
        client.put("foo", "bar");
        assertEquals("get failed", client.get("foo"), "bar");
    }
    
    @Test(timeout = 15000)
    public void testMultiplePutGet() throws KVException {
    	try {
	        client.put("foo", "bar");
	        assertEquals("get failed", client.get("foo"), "bar");
	        client.put("foo", "okay");
	        assertEquals(client.get("foo"), "okay");
	        assertNull(client.get("okay"));
	        client.del("foo");
	        assertNull(client.get("foo"));
	        client.put("foo", "1");
	        assertEquals("1", client.get("foo"));
	        client.put("okay", "correct");
	        assertEquals("correct", client.get("okay"));
    	} catch (Exception e) {
    		fail("Error Occurred!");
    	}
    }

}
