package kvstore;

import static kvstore.KVConstants.*;

import java.util.concurrent.locks.Lock;

/**
 * This class services all storage logic for an individual key-value server.
 * All KVServer request on keys from different sets must be parallel while
 * requests on keys from the same set should be serial. A write-through
 * policy should be followed when a put request is made.
 */
public class KVServer implements KeyValueInterface {

    private KVStore dataStore;
    private KVCache dataCache;

    private static final int MAX_KEY_SIZE = 256;
    private static final int MAX_VAL_SIZE = 256 * 1024;

    /**
     * Constructs a KVServer backed by a KVCache and KVStore.
     *
     * @param numSets the number of sets in the data cache
     * @param maxElemsPerSet the size of each set in the data cache
     */

    public KVServer(int numSets, int maxElemsPerSet) {
        this.dataCache = new KVCache(numSets, maxElemsPerSet);
        this.dataStore = new KVStore();
    }

    /**
     * Performs put request on cache and store.
     *
     * @param  key String key
     * @param  value String value
     * @throws KVException if key or value is too long
     */
    @Override
    public void put(String key, String value) throws KVException {
        if(key.length() > MAX_KEY_SIZE) {
        	KVMessage msg = new KVMessage(KVConstants.RESP, ERROR_OVERSIZED_KEY);
            throw new KVException(msg);
        }
        if(value.length() > MAX_VAL_SIZE) {
        	KVMessage msg = new KVMessage(KVConstants.RESP, ERROR_OVERSIZED_VALUE);
            throw new KVException(msg);
        }
        Lock lock = dataCache.getLock(key);
        try {
        	// In case of some unexpected exception thrown here
        	lock.lock();
        	dataCache.put(key, value);
        	dataStore.put(key, value);
        } finally {
        	lock.unlock();
        }
    }

    /**
     * Performs get request.
     * Checks cache first. Updates cache if not in cache but located in store.
     *
     * @param  key String key
     * @return String value associated with key
     * @throws KVException with ERROR_NO_SUCH_KEY if key does not exist in store
     */
    @Override
    public String get(String key) throws KVException {
    	if(key.length() > MAX_KEY_SIZE) {
        	KVMessage msg = new KVMessage(KVConstants.RESP, ERROR_NO_SUCH_KEY);
            throw new KVException(msg);
        }
    	
    	Lock lock = dataCache.getLock(key);
    	String ret = null;
    	try {
    		lock.lock();
        	ret = dataCache.get(key);
    		if(ret == null) {
    			ret = dataStore.get(key);
    			if(ret != null) 
    				dataCache.put(key, ret);
    		}
    	}
    	finally{
    		lock.unlock();
    	}
    	
    	if(ret == null)
    		throw new KVException(KVConstants.ERROR_NO_SUCH_KEY);
        return ret;
    }

    /**
     * Performs del request.
     *
     * @param  key String key
     * @throws KVException with ERROR_NO_SUCH_KEY if key does not exist in store
     */
    @Override
    public void del(String key) throws KVException {
    	if(key.length() > MAX_KEY_SIZE) {
        	KVMessage msg = new KVMessage(KVConstants.RESP, ERROR_NO_SUCH_KEY);
            throw new KVException(msg);
        }
    	
    	Lock lock = dataCache.getLock(key);
        try {
        	lock.lock();
            dataCache.del(key);
        	dataStore.del(key);
        }
        finally{
        	lock.unlock();
        }
    }

    /**
     * Check if the server has a given key. This is used for TPC operations
     * that need to check whether or not a transaction can be performed but
     * you don't want to modify the state of the cache by calling get(). You
     * are allowed to call dataStore.get() for this method.
     *
     * @param key key to check for membership in store
     */
    public boolean hasKey(String key) {
        try {
        	dataStore.get(key);
        } catch (KVException e) { // an exception of key_not_found is caught
        	return false;
        }
        return true;
    }
    
    /**
     * Added by : Yi Wu
     * Check whether the put request (key, value) is valid
     * 
     * @param key Key of the put request
     * @param value Value of the put request
     * @return Validness of the request
     * @throws KVException containing the error message
     */
    public boolean isValidPut(String key, String value) throws KVException {
    	if(key == null || key.length() == 0)
    		throw new KVException(KVConstants.ERROR_INVALID_KEY);
    	if(value == null || value.length() == 0)
    		throw new KVException(KVConstants.ERROR_INVALID_VALUE);
    	if(key.length() > MAX_KEY_SIZE) 
    		throw new KVException(KVConstants.ERROR_OVERSIZED_KEY);
    	if(value.length() > MAX_VAL_SIZE)
    		throw new KVException(KVConstants.ERROR_OVERSIZED_VALUE);
    	return true;
    }

    /** This method is purely for convenience and will not be tested. */
    @Override
    public String toString() {
        return dataStore.toString() + dataCache.toString();
    }

}
