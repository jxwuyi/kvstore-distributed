package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;
/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 */
public class TPCMasterHandler implements NetworkHandler {

    private long slaveID;
    private KVServer kvServer;
    private TPCLog tpcLog;
    private ThreadPool threadpool;

    /**
     * Constructs a TPCMasterHandler with one connection in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log) {
        this(slaveID, kvServer, log, 1);
    }

    /**
     * Constructs a TPCMasterHandler with a variable number of connections
     * in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     * @param connections the number of connections in this slave's ThreadPool
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log, int connections) {
        this.slaveID = slaveID;
        this.kvServer = kvServer;
        this.tpcLog = log;
        this.threadpool = new ThreadPool(connections);
    }

    /**
     * Registers this slave server with the master.
     *
     * @param masterHostname
     * @param server SocketServer used by this slave server (which contains the
     *               hostname and port this slave is listening for requests on
     * @throws KVException with ERROR_INVALID_FORMAT if the response from the
     *         master is received and parsed but does not correspond to a
     *         success as defined in the spec OR any other KVException such
     *         as those expected in KVClient in project 3 if unable to receive
     *         and/or parse message
     */
    public void registerWithMaster(String masterHostname, SocketServer server)
            throws KVException {
    	String addr = Long.toString(slaveID)+"@"+server.getHostname()+Integer.toString(server.getPort()); 
    	KVMessage reg = new KVMessage(KVConstants.REGISTER, addr);
    	Socket sock = null;
    	try {
    		// Note: By instruction, master always listern for registration at port 9090
    		sock = new Socket(masterHostname, 9090); // TODO: rewrite 9090 by some constant number
    	} catch(IOException e) {
    		throw new KVException(KVConstants.ERROR_COULD_NOT_CONNECT);
    	} catch(Exception e) {
    		throw new KVException(KVConstants.ERROR_COULD_NOT_CREATE_SOCKET);
    	} 
    	
    	try {
	    	reg.sendMessage(sock); // send register request
	    	
	    	KVMessage resp = new KVMessage(sock); // receive response
	    	String expectedMsg = "Successfully registered " + addr;
	    	if (!KVConstants.RESP.equals(resp.getMsgType())
	    		|| !expectedMsg.equals(resp.getMessage())) {
	    		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	    	}
    	} finally {
    		try {
				sock.close();
			} catch (IOException e) {
				// ignore, best effort
			}
    	}
    	
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param master Socket connected to the master with the request
     */
    @Override
    public void handle(Socket master) {
        // implement me
    }

    /**
     * Runnable class containing routine to service a message from the master.
     */
    private class MasterHandler implements Runnable {

        private Socket master;

        /**
         * Construct a MasterHandler.
         *
         * @param master Socket connected to master with the message
         */
        public MasterHandler(Socket master) {
            this.master = master;
        }

        /**
         * Processes request from master and sends back a response with the
         * result. This method needs to handle both phase1 and phase2 messages
         * from the master. The delivery of the response is best-effort. If
         * we are unable to return any response, there is nothing else we can do.
         */
        @Override
        public void run() {
            // implement me
        }

    }

}
