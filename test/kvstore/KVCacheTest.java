package kvstore;

import static org.junit.Assert.*;

import java.util.concurrent.locks.Lock;

import org.junit.*;

public class KVCacheTest {

    /**
     * Verify the cache can put and get a KV pair successfully.
     */
    @Test
    public void singlePutAndGet() {
        KVCache cache = new KVCache(1, 4);
        cache.put("hello", "world");
        assertEquals("world", cache.get("hello"));
    }
    
    @Test
    public void singleGet() {
        KVCache cache = new KVCache(1, 4);
        assertNull(cache.get("hello"));
    }
    
    @Test
    public void multiplePutsAndGets() {
        KVCache cache = new KVCache(1, 4);
        cache.put("hello", "world");
        assertEquals("world", cache.get("hello"));
        cache.put("hello", "world!");
        assertEquals("world!", cache.get("hello"));
    }
    
    @Test
    public void testEviction() {
        KVCache cache = new KVCache(1, 3);
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");
        cache.put("D", "4");
        assertNull(cache.get("A"));
        cache.put("B", "5");
        assertEquals("5", cache.get("B"));
        cache.put("E", "6");
        assertNull(cache.get("C"));
    }

    //==============================================================
    
    @Test
    public void oneByOneCache() {
        KVCache cache = new KVCache(1, 1);
        cache.put("hello", "world");
        assertEquals("world", cache.get("hello"));
        cache.put("0", "233333");
        assertNull(cache.get("hello"));
        assertEquals("233333", cache.get("0"));
    }
    
    @Test
    public void testDeleteCache() {
        KVCache cache = new KVCache(1, 3);
        cache.put("1", "one");
        cache.put("2", "two");
        cache.put("3", "three");
        cache.del("1");
        assertNull(cache.get("1"));
        cache.put("4", "four");
        assertEquals("two", cache.get("2"));
        assertEquals("three", cache.get("3"));
        assertEquals("four", cache.get("4"));
        cache.del("5");
        assertEquals("two", cache.get("2"));
        assertEquals("three", cache.get("3"));
        assertEquals("four", cache.get("4"));
    }
    // It is not specified that after deletion, how the queue looks like...
    
	@Test
	public void testMutilpleSets() {
		KVCache cache= new KVCache(2, 1);
		cache.put("1", "one");
		cache.put("2", "two");
		cache.put("3", "three");
		assertNull(cache.get("1"));
		assertEquals("two", cache.get("2"));
		assertEquals("three", cache.get("3"));
	}
    
	@Test
	public void testLock() {
		KVCache cache= new KVCache(2, 1);
		cache.put("1", "one");
		Lock lock = cache.getLock("1");
		lock.lock();
	}
	// TODO: Multiple Threads in order to test locks...
	
	@Test
	public void testToXML() throws Exception {
		KVCache cache = new KVCache(3, 2);

		// test empty, what is the required cache.toXML() ?
		// Minor Problem: The property of "standalone" is not shown in the example
		assertEquals(
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\r\n"+
				"<KVCache>\r\n" +
				"<Set Id=\"0\"/>\r\n" +
				"<Set Id=\"1\"/>\r\n" +
				"<Set Id=\"2\"/>\r\n" +
				"</KVCache>\r\n",
				cache.toXML()
				);

		
		cache.put("1", "ones");	
		// TODO: test single element, cache.toXML()?
		
		// test full cache
		cache.put("2", "two");
		cache.put("3", "three");
		cache.put("4", "four");
		cache.put("5", "five");
		cache.put("6", "six");
		assertEquals(
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\r\n"+
				"<KVCache>\r\n" +
				"<Set Id=\"0\">\r\n" +
				"<CacheEntry isReferenced=\"false\">\r\n" +
				"<Key>3</Key>\r\n" +
				"<Value>three</Value>\r\n" +
				"</CacheEntry>\r\n" +
				"<CacheEntry isReferenced=\"false\">\r\n" +
				"<Key>6</Key>\r\n" +
				"<Value>six</Value>\r\n" +
				"</CacheEntry>\r\n" +
				"</Set>\r\n" +
				"<Set Id=\"1\">\r\n" +
				"<CacheEntry isReferenced=\"false\">\r\n" +
				"<Key>1</Key>\r\n" +
				"<Value>ones</Value>\r\n" +
				"</CacheEntry>\r\n" +
				"<CacheEntry isReferenced=\"false\">\r\n" +
				"<Key>4</Key>\r\n" +
				"<Value>four</Value>\r\n" +
				"</CacheEntry>\r\n" +
				"</Set>\r\n" +
				"<Set Id=\"2\">\r\n" +
				"<CacheEntry isReferenced=\"false\">\r\n" +
				"<Key>2</Key>\r\n" +
				"<Value>two</Value>\r\n" +
				"</CacheEntry>\r\n" +
				"<CacheEntry isReferenced=\"false\">\r\n" +
				"<Key>5</Key>\r\n" +
				"<Value>five</Value>\r\n" +
				"</CacheEntry>\r\n" +
				"</Set>\r\n" +
				"</KVCache>\r\n", 
				cache.toXML()
				);
	}
}
