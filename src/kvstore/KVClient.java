package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;

/**
 * Client API used to issue requests to key-value server.
 */
public class KVClient implements KeyValueInterface {

    private String server;
    private int port;

    /**
     * Constructs a KVClient connected to a server.
     *
     * @param server is the DNS reference to the server
     * @param port is the port on which the server is listening
     */
    public KVClient(String server, int port) {
        this.server = server;
        this.port = port;
    }

    /**
     * Creates a socket connected to the server to make a request.
     *
     * @return Socket connected to server
     * @throws KVException if unable to create or connect socket
     */
    private Socket connectHost() throws KVException {
    	Socket sock = null;
    	try {
    		sock = new Socket(server, port);
    	} catch(IOException e) {
    		throw new KVException(KVConstants.ERROR_COULD_NOT_CONNECT);
    	} catch(Exception e) {
    		throw new KVException(KVConstants.ERROR_COULD_NOT_CREATE_SOCKET);
    	}
        return sock;
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param  sock Socket to be closed
     */
    private void closeHost(Socket sock) {
    	try {
			sock.close();
		} catch (Exception e) {
			//best effort
		}
    }

    /**
     * Issues a PUT request to the server.
     *
     * @param  key String to put in server as key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void put(String key, String value) throws KVException {
    	// check validness of key and value
    	if (key == null || key.length() == 0)
    		throw new KVException(KVConstants.ERROR_INVALID_KEY);
    	if (value == null || value.length() == 0)
    		throw new KVException(KVConstants.ERROR_INVALID_VALUE);
    	
    	// Send Request
    	KVMessage msg = new KVMessage(KVConstants.PUT_REQ);
    	msg.setKey(key);
    	msg.setValue(value);
    	
    	Socket sock = null;
    	try {
    		sock = connectHost();
    		msg.sendMessage(sock);
    	
    		// Receive Response
    		new KVMessage(sock); // we don't need to print the success msg
    	} finally {
    		if(sock != null) closeHost(sock);
    	}
    }

    /**
     * Issues a GET request to the server.
     *
     * @param  key String to get value for in server
     * @return String value associated with key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public String get(String key) throws KVException {
    	// check validness of key
    	if (key == null || key.length() == 0)
    		throw new KVException(KVConstants.ERROR_INVALID_KEY);
    	
    	// Send Request
    	KVMessage msg = new KVMessage(KVConstants.GET_REQ);
    	msg.setKey(key);
    	
    	Socket sock = null;
    	try {
	    	sock = connectHost();
	    	msg.sendMessage(sock);
	    	
	    	// Receive Response
	    	KVMessage resp = new KVMessage(sock);
	    	return resp.getValue();
    	} finally {
    		if(sock != null) closeHost(sock);
    	}
    }

    /**
     * Issues a DEL request to the server.
     *
     * @param  key String to delete value for in server
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void del(String key) throws KVException {
    	// check validness of key
    	if (key == null || key.length() == 0)
    		throw new KVException(KVConstants.ERROR_INVALID_KEY);
    	
    	// Send Request
    	KVMessage msg = new KVMessage(KVConstants.DEL_REQ);
    	msg.setKey(key);
    	
    	Socket sock = null;
    	try {
    		sock = connectHost();
    		msg.sendMessage(sock);
    	
    		// Receive Response
    		new KVMessage(sock); // we don't need to print the success msg
    	} finally {
    		if(sock != null) closeHost(sock);
    	}
    }


}
