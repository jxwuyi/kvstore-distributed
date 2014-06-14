package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of its methods are blocking.
 */
public class ServerClientHandler implements NetworkHandler {

    private KVServer kvServer;
    private ThreadPool threadPool;

    /**
     * Constructs a ServerClientHandler with ThreadPool of a single thread.
     *
     * @param kvServer KVServer to carry out requests
     */
    public ServerClientHandler(KVServer kvServer) {
        this(kvServer, 1);
    }

    /**
     * Constructs a ServerClientHandler with ThreadPool of thread equal to
     * the number passed in as connections.
     *
     * @param kvServer KVServer to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public ServerClientHandler(KVServer kvServer, int connections) {
        this.kvServer = kvServer;
        threadPool = new ThreadPool(connections);
    }

    /**
     * Creates a job to service the request for a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
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
     * Runnable class with routine to service a request from the client.
     */
    private class ClientHandler implements Runnable {

        private Socket client;

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
         * unable to return a response, there is nothing else we can do.
         */
        @Override
        public void run() {
        	KVMessage resp = null;
            try {
				KVMessage msg = new KVMessage(client);
				if(msg.getMsgType().equals(KVConstants.PUT_REQ)) { // put
					kvServer.put(msg.getKey(), msg.getValue());
					resp = new KVMessage(KVConstants.RESP,KVConstants.SUCCESS);
				} else
				if(msg.getMsgType().equals(KVConstants.GET_REQ)) { // get
					String value = kvServer.get(msg.getKey());
					resp = new KVMessage(KVConstants.RESP);
					resp.setKey(msg.getKey());
					resp.setValue(value);
				} else
				if(msg.getMsgType().equals(KVConstants.DEL_REQ)) { // del
					kvServer.del(msg.getKey());
					resp = new KVMessage(KVConstants.RESP,KVConstants.SUCCESS);
				} else
					// no such key
					return ;
			} catch (KVException e) {
				// send back an error message
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
