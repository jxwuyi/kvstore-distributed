package kvstore;

import static kvstore.KVConstants.ERROR_NO_SUCH_KEY;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.*;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KVServer.class)
public class KVServerTest {

    KVServer server;
    static KVCache realCache;
    KVCache mockCache;
    static KVStore realStore;
    KVStore mockStore;

    /**
     * Nick: This is necessary because once I start mocking constructors, I
     * haven't figured out a way to use the real ones again.  Also, this sucks
     * because the cache doesn't get reset between tests -_- there is no method
     * for that, though and I can't construct a new cache with the actual
     * constructor.  Only fix I thought of was creating an array of new caches
     * and using a different one for each test, but that's even worse imo :(
     */
    @BeforeClass
    public static void setupRealDependencies() {
        realCache = new KVCache(10, 10);
        realStore = new KVStore();
    }

    public void setupRealServer() {
        try {
            whenNew(KVCache.class).withArguments(anyInt(), anyInt()).thenReturn(realCache);
            whenNew(KVStore.class).withNoArguments().thenReturn(realStore);
            server = new KVServer(10, 10);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setupMockServer() {
        try {
            mockCache = mock(KVCache.class);
            mockStore = mock(KVStore.class);
            whenNew(KVCache.class).withAnyArguments().thenReturn(mockCache);
            whenNew(KVStore.class).withAnyArguments().thenReturn(mockStore);
            server = new KVServer(10, 10);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void fuzzTest() throws KVException {
        setupRealServer();
        Random rand = new Random(8); // no reason for 8
        Map<String, String> map = new HashMap<String, String>(10000);
        String key, val;
        for (int i = 0; i < 10000; i++) {
            key = Integer.toString(rand.nextInt());
            val = Integer.toString(rand.nextInt());
            server.put(key, val);
            map.put(key, val);
        }
        Iterator<Map.Entry<String, String>> mapIter = map.entrySet().iterator();
        Map.Entry<String, String> pair;
        while(mapIter.hasNext()) {
            pair = mapIter.next();
            assertTrue(server.hasKey(pair.getKey()));
            assertEquals(pair.getValue(), server.get(pair.getKey()));
            mapIter.remove();
        }
        assertTrue(map.size() == 0);
    }

    @Test
    public void testNonexistentGetFails() {
        setupRealServer();
        try {
            server.get("this key shouldn't be here");
            fail("get with nonexistent key should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_NO_SUCH_KEY, e.getKVMessage().getMessage());
        }
    }
    
    @Test
    public void testOversizedKeyPutFails() {
        setupRealServer();
        try {
        	StringBuffer key = new StringBuffer();
        	for(int i=0;i<300;++i) key.append('a');
        	StringBuffer value = new StringBuffer();
        	for(int i=0;i<300;++i) value.append('b');
            server.put(key.toString(),value.toString());
            fail("put with oversized key should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_OVERSIZED_KEY, e.getKVMessage().getMessage());
        }
    }
    
    @Test
    public void testOversizedValuePutFails() {
        setupRealServer();
        try {
        	StringBuffer key = new StringBuffer();
        	for(int i=0;i<20;++i) key.append('a');
        	StringBuffer value = new StringBuffer();
        	for(int i=0;i<260*1024;i++) value.append('b');
            server.put(key.toString(),value.toString());
            fail("put with oversized value should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_OVERSIZED_VALUE, e.getKVMessage().getMessage());
        }
    }


}
