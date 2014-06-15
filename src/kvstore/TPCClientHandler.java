package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * It uses a threadPool to ensure that none of it's methods are blocking.
 */
public class TPCClientHandler implements NetworkHandler {

    private TPCMaster tpcMaster;
    private ThreadPool threadPool;

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     */
    public TPCClientHandler(TPCMaster tpcMaster) {
        this(tpcMaster, 1);
    }

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public TPCClientHandler(TPCMaster tpcMaster, int connections) {
    	this.tpcMaster = tpcMaster;
    	threadPool = new ThreadPool(connections);
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore InterruptedExceptions.
     *
     * @param client Socket connected to the client with the request
     */
    @Override
    public void handle(Socket client) {
    	try {
			threadPool.addJob(new ClientHandler(client));
		} catch (InterruptedException e) {
			// ignore
		}
    }

    /**
     * Runnable class containing routine to service a request from the client.
     */
    private class ClientHandler implements Runnable {

        private Socket client = null;

        /**
         * Construct a ClientHandler.
         *
         * @param client Socket connected to client with the request
         */
        public ClientHandler(Socket client) {
            this.client = client;
        }

        /**
         * Processes request from client and sends back a response with the
         * result. The delivery of the response is best-effort. If we are
         * unable to return any response, there is nothing else we can do.
         */
        @Override
        public void run() {
        	KVMessage resp = null;
        	try {
				KVMessage req = new KVMessage(client);
				if(KVConstants.GET_REQ.equals(req.getMsgType())) { // get
					String value = tpcMaster.handleGet(req);
					resp = new KVMessage(KVConstants.RESP);
					resp.setKey(req.getKey());
					resp.setValue(value);
				} else
				if(KVConstants.PUT_REQ.equals(req.getMsgType())) { // put
					tpcMaster.handleTPCRequest(req, true);
					resp = new KVMessage(KVConstants.RESP,KVConstants.SUCCESS);
				} else
				if(KVConstants.DEL_REQ.equals(req.getMsgType())) { // del
					tpcMaster.handleTPCRequest(req, false);
					resp = new KVMessage(KVConstants.RESP,KVConstants.SUCCESS);
				}
			} catch (KVException e) {
				resp = new KVMessage(e.getKVMessage());
			}
        	if(resp != null) {
            	try {
					resp.sendMessage(client);
				} catch (KVException e) {
					// nothing can be done
				}
            }
        }
    }

}
