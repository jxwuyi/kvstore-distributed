package kvstore;

import static org.junit.Assert.*;

import org.junit.Test;

public class EndToEndTest extends EndToEndTemplate {

    @Test
    public void testPutGet() throws KVException {
        client.put("foo", "bar");
        assertEquals(client.get("foo"), "bar");
    }
    
	@Test
    public void testModification() throws KVException {
        client.put("a", "1");
        client.put("a", "2");
        assertEquals(client.get("a"), "2");  
    }
	
	@Test
    public void testModificationMoreTimes() throws KVException {
		int i;
        for (i = 0; i < 1000; ++i ) {
        	client.put("a", Integer.toString(i));
        }
        assertEquals(client.get("a"), Integer.toString(i-1));  
    }
	

	@Test
    public void simpleDeleteTest() throws KVException {
		client.put("goood", "do");
		assertEquals(client.get("goood"), "do");
		client.del("goood");
		try {
			client.get("goood");
			fail("NO_SUCH_KEY Exception not received!");
		} catch(KVException e) {
			assertEquals(KVConstants.ERROR_NO_SUCH_KEY, e.getKVMessage().getMessage());
		}
    }
	
	@Test
    public void deleteNoneExistKey() throws KVException {
		try {
			client.del("goood");
			fail("NO_SUCH_KEY Exception not received!");
		} catch(KVException e) {
			assertEquals(KVConstants.ERROR_NO_SUCH_KEY, e.getKVMessage().getMessage());
		}
		try {
			client.get("goood");
			fail("NO_SUCH_KEY Exception not received!");
		} catch(KVException e) {
			assertEquals(KVConstants.ERROR_NO_SUCH_KEY, e.getKVMessage().getMessage());
		}
    }

}
