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
	        try {
	        	client.get("okay");
	        	fail("NO_SUCH_KEY Exception not thrown!");
	        } catch(KVException e) {
	        	assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
	    		assertEquals(KVConstants.ERROR_NO_SUCH_KEY, e.getKVMessage().getMessage());
	        }
	        client.del("foo");
	        try {
	        	client.get("foo");
	        	fail("NO_SUCH_KEY Exception not thrown!");
	        } catch(KVException e) {
	        	assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
	    		assertEquals(KVConstants.ERROR_NO_SUCH_KEY, e.getKVMessage().getMessage());
	        }
	        client.put("foo", "1");
	        assertEquals("1", client.get("foo"));
	        client.put("okay", "correct");
	        assertEquals("correct", client.get("okay"));
    	} catch (Exception e) {
    		fail("Error Occurred! message = " + e.getMessage());
    	}
    }

}
