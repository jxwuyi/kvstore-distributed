package kvstore;

import static kvstore.KVConstants.*;
import static org.junit.Assert.*;

import org.junit.*;

public class KVStoreTest {

    KVStore store;

    @Before
    public void setupStore() {
        store = new KVStore();
    }

    @Test
    public void putAndGetOneKey() throws KVException {
        String key = "this is the key.";
        String val = "this is the value.";
        store.put(key, val);
        assertEquals(val, store.get(key));
    }

}
