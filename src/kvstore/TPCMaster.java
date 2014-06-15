package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class TPCMaster {

    private int numSlaves;
    private KVCache masterCache;

    public static final int TIMEOUT = 3000;

    ArrayList<TPCSlaveInfo> slaves;
    
    /**
     * Creates TPCMaster, expecting numSlaves slave servers to eventually register
     *
     * @param numSlaves number of slave servers expected to register
     * @param cache KVCache to cache results on master
     */
    public TPCMaster(int numSlaves, KVCache cache) {
        this.numSlaves = numSlaves;
        this.masterCache = cache;
        // implement me
        
        slaves = new ArrayList<TPCSlaveInfo>();
    }

    /**
     * Registers a slave. Drop registration request if numSlaves already
     * registered.Note that a slave re-registers under the same slaveID when
     * it comes back online.
     *
     * @param slave the slaveInfo to be registered
     * @return whether the slave is successfully registered (modified by: Yi Wu)
     */
    public boolean registerSlave(TPCSlaveInfo slave) {
    	// TODO: I slightly modified the API here!
    	//       original version is void, now I change to boolean
    	synchronized(this) {
    		for(int i=0;i<slaves.size();++i) {
    			if(slaves.get(i).getSlaveID() == slave.getSlaveID()) {
    				slaves.set(i, slave);
    				return true;
    			}
    			if(isLessThanUnsigned(slave.getSlaveID(), slaves.get(i).getSlaveID())) {
    				if(slaves.size() == numSlaves) return false;
    				slaves.add(i, slave);
    				return true;
    			}
    		}
    		if(slaves.size() == numSlaves) return false;
    		slaves.add(slave); // add the slave at the end of the array
    		return true;
    	}
    }

    /**
     * Converts Strings to 64-bit longs. Borrowed from http://goo.gl/le1o0W,
     * adapted from String.hashCode().
     *
     * @param string String to hash to 64-bit
     * @return long hashcode
     */
    public static long hashTo64bit(String string) {
        long h = 1125899906842597L;
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = (31 * h) + string.charAt(i);
        }
        return h;
    }

    /**
     * Compares two longs as if they were unsigned (Java doesn't have unsigned
     * data types except for char). Borrowed from http://goo.gl/QyuI0V
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than unsigned n2
     */
    public static boolean isLessThanUnsigned(long n1, long n2) {
        return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
    }

    /**
     * Compares two longs as if they were unsigned, uses isLessThanUnsigned
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than or equal to unsigned n2
     */
    public static boolean isLessThanEqualUnsigned(long n1, long n2) {
        return isLessThanUnsigned(n1, n2) || (n1 == n2);
    }

    /**
     * added by: Yi Wu
     * find the index of the primary replica for a given key
     *   using binary search
     * 
     * @param hash value of the input hey
     * @return the index of the desired replica, -1 if no replica available
     */
    private int findFirstReplicaIndex(long hash) {
    	if (slaves.size() <= 1) return slaves.size() - 1;
    	if(isLessThanEqualUnsigned(hash, slaves.get(0).getSlaveID())
    		|| !isLessThanEqualUnsigned(hash, slaves.get(slaves.size()-1).getSlaveID()))
    		return 0;
    	int lo = 0, hi = slaves.size() - 1, mid; // binary search
    	while(lo + 1 < hi) {
    		mid = (lo + hi) >> 1;
    		if(isLessThanEqualUnsigned(hash, slaves.get(mid).getSlaveID()))
    			hi = mid;
    		else
    			lo = mid;
    	}
    	return hi;
    }
    
    /**
     * Find primary replica for a given key.
     *
     * @param key String to map to a slave server replica
     * @return SlaveInfo of first replica
     */
    public TPCSlaveInfo findFirstReplica(String key) {
    	int pos = findFirstReplicaIndex(hashTo64bit(key));
    	if(pos < 0) return null;
        return slaves.get(pos);
    }

    /**
     * Find the successor of firstReplica.
     *
     * @param firstReplica SlaveInfo of primary replica
     * @return SlaveInfo of successor replica
     */
    public TPCSlaveInfo findSuccessor(TPCSlaveInfo firstReplica) {
        int pos = findFirstReplicaIndex(firstReplica.getSlaveID());
        if(pos < 0) return null;
        pos ++;
        if(pos == slaves.size()) pos = 0;
        return slaves.get(pos);
    }

    /**
     * Perform 2PC operations from the master node perspective. This method
     * contains the bulk of the two-phase commit logic. It performs phase 1
     * and phase 2 with appropriate timeouts and retries.
     *
     * See the spec for details on the expected behavior.
     *
     * @param msg KVMessage corresponding to the transaction for this TPC request
     * @param isPutReq boolean to distinguish put and del requests
     * @throws KVException if the operation cannot be carried out for any reason
     */
    public synchronized void handleTPCRequest(KVMessage msg, boolean isPutReq)
            throws KVException {
        // implement me
    }

    /**
     * Perform GET operation in the following manner:
     * - Try to GET from cache, return immediately if found
     * - Try to GET from first/primary replica
     * - If primary succeeded, return value
     * - If primary failed, try to GET from the other replica
     * - If secondary succeeded, return value
     * - If secondary failed, return KVExceptions from both replicas
     *
     * @param msg KVMessage containing key to get
     * @return value corresponding to the Key
     * @throws KVException with ERROR_NO_SUCH_KEY if unable to get
     *         the value from either slave for any reason
     */
    public String handleGet(KVMessage msg) throws KVException {
        // implement me
        return null;
    }

}
