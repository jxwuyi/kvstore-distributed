package kvstore;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TPCEndToEndTest extends TPCEndToEndTemplate {

    @Test(timeout = 15000)
    public void testPutGet() throws KVException {
        client.put("foo", "bar");
        assertEquals("get failed", client.get("foo"), "bar");
    }

}
