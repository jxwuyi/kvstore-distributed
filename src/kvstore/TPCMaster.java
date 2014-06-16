package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class TPCMaster {

    private int numSlaves;
    private KVCache masterCache;

    public static final int TIMEOUT = 3000;

    ArrayList<TPCSlaveInfo> slaves;
    boolean isBlocked;
    
    /**
     * Creates TPCMaster, expecting numSlaves slave servers to eventually register
     *
     * @param numSlaves number of slave servers expected to register
     * @param cache KVCache to cache results on master
     */
    public TPCMaster(int numSlaves, KVCache cache) {
        this.numSlaves = numSlaves;
        this.masterCache = cache;
        
        // isBlocked will be false when slaves.size() == numSlaves
        isBlocked = true;
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
    	synchronized(slaves) { // NOTE: important to lock slaves!
    		for(int i=0;i<slaves.size();++i) {
    			if(slaves.get(i).getSlaveID() == slave.getSlaveID()) {
    				slaves.set(i, slave);
    				return true;
    			}
    			if(isLessThanUnsigned(slave.getSlaveID(), slaves.get(i).getSlaveID())) {
    				if(slaves.size() == numSlaves) return false;
    				slaves.add(i, slave);
    				if(slaves.size() == numSlaves) isBlocked = false;
    				return true;
    			}
    		}
    		if(slaves.size() == numSlaves) return false;
    		slaves.add(slave); // add the slave at the end of the array
    		if(slaves.size() == numSlaves) isBlocked = false;
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
    	while(isBlocked) { // make sure blocked before getting enough slaves
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// ignore
			}
    	}
    	
    	String key = msg.getKey();
    	Lock lock = masterCache.getLock(key);
    	try {
    		lock.lock();
    		
    		// phase-1 commit
    		int repInd[] = new int[2];
    		repInd[0] = findFirstReplicaIndex(hashTo64bit(key));
    		repInd[1] = ( repInd[0] + 1 ) % numSlaves;
    		// TODO: run the following code concurrently
    		boolean commit = true;
    		try {
    			// sequentially send request to replicas
    			for(int i = 0; i < repInd.length; ++ i) {
    				Socket sock = null;
    				TPCSlaveInfo slave = slaves.get(repInd[i]);
    				try {
    					sock = slave.connectHost(TIMEOUT);
    					msg.sendMessage(sock); // send request
    					KVMessage resp = new KVMessage(sock); // receive response
    					if(!KVConstants.READY.equals(resp.getMsgType())) // not ready
    						commit = false;
    				} finally {
    					if(sock != null) {
    						slave.closeHost(sock);
    					}
    				}
    			}
    		} catch(Exception e) {
    			commit = false;
    		}
    		
    		// phase-2 commit
    		KVMessage decision = null;
    		if(commit) {
    			decision = new KVMessage(KVConstants.COMMIT);
    			
    			// update Cache
    			if(isPutReq)
    				masterCache.put(msg.getKey(), msg.getValue()); // put
    			else
    				masterCache.del(msg.getKey()); // del
    		}
    		else decision = new KVMessage(KVConstants.ABORT);
    		
    		for(int i=0;i<repInd.length;++i) {
    			// NOTE: have to get every time! The object in slaves may be replaced
    			TPCSlaveInfo slave = slaves.get(repInd[i]);
    			while(true) { // send decision until receive an response
    				Socket sock = null;
    				KVMessage resp = null;
    				try {
    					sock = slave.connectHost(TIMEOUT);
    					decision.sendMessage(sock);
    					resp = new KVMessage(sock);
    				} catch(Exception e) {
    					continue; // ignore and continue
    				} finally {
    					if(sock != null)
    						slave.closeHost(sock);
    				}
    				if(KVConstants.ACK.equals(resp.getMsgType()))
    					break;
    				
    				// print to the console
    				System.err.println("Internal Error: replica replied <"+resp.getMsgType()+"> instead of <ACK> in phase-2 commits!");
    				throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
    			}
    		}
    	} finally {
    		lock.unlock();
    	}
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
    	while(isBlocked) { // make sure blocked before getting enough slaves
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// ignore
			}
    	}
    	
    	String key = msg.getKey();
    	Lock lock = masterCache.getLock(key);
    	String ret = null;
    	try {
    		lock.lock();
    		ret = masterCache.get(key); // get from cache
    		if(ret == null) {
    			TPCSlaveInfo slave = findFirstReplica(key); // primary replica
    			ret = getFromReplica(msg, slave);
    			if(ret == null) {
    				slave = findSuccessor(slave); // secondary replica
    				ret = getFromReplica(msg, slave);
    			}
    			// update Cache
    			if(ret != null)
    				masterCache.put(key, ret);
    		}
    	} finally {
    		lock.unlock();
    	}
        if(ret == null)
        	throw new KVException(KVConstants.ERROR_NO_SUCH_KEY);
        return ret;
    }
    
    /**
     * added by : Yi Wu
     * perform get request at a replica
     * 
     * @param msg Message to send
     * @param slave The replica
     * @return the value, null if no such key
     */
    private String getFromReplica(KVMessage msg, TPCSlaveInfo slave) {
    	String ret = null;
    	Socket sock = null;
    	try {
			sock = slave.connectHost(TIMEOUT);
			msg.sendMessage(sock); // send request
			
			KVMessage resp = new KVMessage(sock);
			if(KVConstants.RESP.equals(resp.getMsgType()) && resp.getValue().length() > 0)
				ret = resp.getValue();
		} catch (Exception e) {
			ret = null;
		} finally {
			if(sock != null) {
				slave.closeHost(sock);
			}
		}
		return ret;
    }
}
