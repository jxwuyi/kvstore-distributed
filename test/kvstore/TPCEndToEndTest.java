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

    @Test(timeout = 60000)
    public void testSingleSlaveCrash() throws KVException {
        client.put(KEY1, "1");
        try {
        	assertEquals("1", client.get(KEY1));
        } catch (Exception e) {
        	fail("Error Occurred! message = " + e.getMessage());
        }
        
        try {
        	stopSlave(Long.toString(SLAVE1));
        } catch(Exception e) {
        	e.printStackTrace();
        }
        
        try {
        	assertEquals("1", client.get(KEY1));
        	client.put(KEY2, "key2");
        	assertEquals("key2", client.get(KEY2));
        } catch (Exception e) {
        	fail("Error Occurred! message = " + e.getMessage());
        }
        
        Thread thread1 = new Thread(
				new Runnable() {
					public void run() {
						try {
							Thread.sleep(10000);
							try {
								startSlave(SLAVE1);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
							try {
								assertEquals("1", client.get(KEY1));
							} catch (KVException e) {
								fail("failed when query KEY1");
							}
							
							try {
								client.put(KEY1, "3");
							} catch (KVException e) {
								fail("failed when updating KEY1 to 3");
							}
							
							try {
								assertEquals("3", client.get(KEY1));
							} catch (KVException e) {
								fail("failed when query KEY1");
							}
						} catch (InterruptedException e) {
							// ignore
						}
					}
				});
        
        Thread thread2 = new Thread(
				new Runnable() {
					public void run() {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e1) {
							// ignore
						}
						try {
							client.put(KEY1, "2");
							fail("No Exception thrown when one replica crashes!");
						} catch(KVException e) {
							
						}
					}
				});
        
        try {
        	client.put(KEY3, "key3");
        } catch(KVException e) {
        	fail("failed on putting KEY3");
        }
        
        thread1.start();
        thread2.start();
        
        try {
			Thread.sleep(300);
		} catch (InterruptedException e1) {
			// ignore
		}
        try {
        	assertEquals("key3", client.get(KEY3));
        } catch(KVException e) {
        	fail("failed on getting value of KEY3");
        }
        try {
        	assertEquals("key2", client.get(KEY2));
        } catch(KVException e) {
        	fail("failed on getting value of KEY2");
        }
        
        while(thread1.isAlive() || thread2.isAlive()) {
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }
}
