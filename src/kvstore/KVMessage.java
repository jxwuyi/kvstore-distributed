package kvstore;

import static kvstore.KVConstants.*;

import java.io.*;
import java.net.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * This is the object that is used to generate the XML based messages
 * for communication between clients and servers.
 */
public class KVMessage implements Serializable {

    private String msgType;
    private String key;
    private String value;
    private String message;

    public static final long serialVersionUID = 6473128480951955693L;

    /**
     * Construct KVMessage with only a type.
     *
     * @param msgType the type of this KVMessage
     */
    public KVMessage(String msgType) {
        this(msgType, null);
    }

    /**
     * Construct KVMessage with type and message.
     *
     * @param msgType the type of this KVMessage
     * @param message the content of this KVMessage
     */
    public KVMessage(String msgType, String message) {
        this.msgType = msgType;
        this.message = message;
    }

    /**
     * Construct KVMessage from the InputStream of a socket.
     * Parse XML from the InputStream with unlimited timeout.
     *
     * @param  sock Socket to receive serialized KVMessage through
     * @throws KVException if we fail to create a valid KVMessage. Please see
     *         KVConstants.java for possible KVException messages.
     */
    public KVMessage(Socket sock) throws KVException {
        this(sock, 0);
    }

    /**
     * Construct KVMessage from the InputStream of a socket.
     * This constructor parses XML from the InputStream within a certain timeout
     * or with an unlimited timeout if the provided argument is 0.
     *
     * @param  sock Socket to receive serialized KVMessage through
     * @param  timeout total allowable receipt time, in milliseconds
     * @throws KVException if we fail to create a valid KVMessage. Please see
     *         KVConstants.java for possible KVException messages.
     */
    public KVMessage(Socket sock, int timeout) throws KVException {
    	try {
			sock.setSoTimeout(timeout); // set Timeout
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	    	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	    	Document doc = dBuilder.parse(new NoCloseInputStream(sock.getInputStream()));
	    	
	    	Element root = doc.getDocumentElement();
	    	
	    	root.normalize(); // normalization is recommended
	    	
	    	msgType = root.getAttribute("type");
	    	if(msgType.equals(KVConstants.PUT_REQ)) { // put
	    		key = doc.getElementsByTagName("Key").item(0).getTextContent();
	    		value = doc.getElementsByTagName("Value").item(0).getTextContent();
	    		
	    		if(key == null || value == null || key.length() == 0 || value.length() == 0)
	    			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	    		
	    	} else
	    	if(msgType.equals(KVConstants.GET_REQ)) { // get
	    		key = doc.getElementsByTagName("Key").item(0).getTextContent();
	    		
	    		if(key == null || key.length() == 0)
	    			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	    		
	    	} else
	    	if(msgType.equals(KVConstants.DEL_REQ)) { // del
	    		key = doc.getElementsByTagName("Key").item(0).getTextContent();
	    		
	    		if(key == null || key.length() == 0)
	    			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	    		
	    	} else
	    	if(msgType.equals(KVConstants.REGISTER)) { // register
	    		message = doc.getElementsByTagName("Message").item(0).getTextContent();
	    		
	    		if(message == null || !message.contains("@") || !message.contains(":"))
	    			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	    	} else
	    	if(msgType.equals(KVConstants.READY)) { // ready vote
	    		// do nothing
		    } else
		    if(msgType.equals(KVConstants.ABORT)) { // abort vote || abort decision
		    	if(doc.getElementsByTagName("Message").getLength() > 0) { // abort vote
		    		message = doc.getElementsByTagName("Message").item(0).getTextContent();
		    		if(message == null)
		    			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
		    	}
		    	// otherwise abort decision
		    } else
		    if(msgType.equals(KVConstants.COMMIT)) { // commit decision
		    	// do nothing
			} else
			if(msgType.equals(KVConstants.ACK)) { // ack
			    // do nothing
			} else
	    	if(msgType.equals(KVConstants.RESP)) { // response
	    		if(doc.getElementsByTagName("Message").getLength() > 0)
	    			message = doc.getElementsByTagName("Message").item(0).getTextContent();
	    		if(doc.getElementsByTagName("Key").getLength() > 0)
	    			key = doc.getElementsByTagName("Key").item(0).getTextContent();
	    		if(doc.getElementsByTagName("Value").getLength() > 0)
	    			value = doc.getElementsByTagName("Value").item(0).getTextContent();
	    		if(message != null) {
	    			if(key != null || value != null)
	    				throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	    		} else {
	    			if(key == null || value == null || key.length() == 0 || value.length() == 0)
	    				throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	    		}
	    	} else
	    		// no such type
	    		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
		} catch (SocketTimeoutException e) { // Exception for Timeout
    		throw new KVException(KVConstants.ERROR_SOCKET_TIMEOUT);
    	} catch (SocketException e) { // Exception when calling setSoTime()
			throw new KVException(KVConstants.ERROR_COULD_NOT_CREATE_SOCKET);
		} catch (ParserConfigurationException e) {
			throw new KVException(KVConstants.ERROR_PARSER);
		} catch (SAXException e) {
			throw new KVException(KVConstants.ERROR_PARSER);
		} catch (IOException e) {
			throw new KVException(KVConstants.ERROR_COULD_NOT_RECEIVE_DATA);
		} catch (KVException e) {
			throw e;
		} catch(Exception e) { // any other exceptions
			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
		}
    }

    /**
     * Constructs a KVMessage by copying another KVMessage.
     *
     * @param kvm KVMessage with fields to copy
     */
    public KVMessage(KVMessage kvm) {
        msgType = kvm.getMsgType();
        key = kvm.getKey();
        value = kvm.getValue();
        message = kvm.getMessage();
    }

    /**
     * Generate the serialized XML representation for this message. See
     * the spec for details on the expected output format.
     *
     * @return the XML string representation of this KVMessage
     * @throws KVException with ERROR_INVALID_FORMAT or ERROR_PARSER
     */
    public String toXML() throws KVException {
    	DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = null;
		try {
			docBuilder = docFactory.newDocumentBuilder();
		}catch (ParserConfigurationException e) {
			throw new KVException(KVConstants.ERROR_PARSER);
		} 
		Document doc = docBuilder.newDocument();

		Element msg = doc.createElement("KVMessage");
		doc.appendChild(msg);
			
		if(msgType == null)
			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
		
		msg.setAttribute("type", msgType);
		
		if(msgType.equals(KVConstants.RESP)) {
			if(message != null) { // only message
    			if(key != null || value != null)
    				throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
    			Element message = doc.createElement("Message");
    			message.appendChild(doc.createTextNode(this.message));
    			msg.appendChild(message);
    		} else { // resp of getreq
    			if(key == null || value == null)
    				throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
    			Element key = doc.createElement("Key");
	    		key.appendChild(doc.createTextNode(this.key));
	    		msg.appendChild(key);
	    		Element value = doc.createElement("Value");
	    		value.appendChild(doc.createTextNode(this.value));
	    		msg.appendChild(value);
    		}
		} else {
			if(msgType.equals(KVConstants.PUT_REQ)) { // put
				if(key == null)
	    			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
				
				Element key = doc.createElement("Key");
	    		key.appendChild(doc.createTextNode(this.key));
	    		msg.appendChild(key);
	
	    		if(value == null)
	    			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	    		Element value = doc.createElement("Value");
	    		value.appendChild(doc.createTextNode(this.value));
	    		msg.appendChild(value);
			} else
			if(msgType.equals(KVConstants.GET_REQ)) { // get
				if(key == null)
	    			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
				
				Element key = doc.createElement("Key");
	    		key.appendChild(doc.createTextNode(this.key));
	    		msg.appendChild(key);
			} else
			if(msgType.equals(KVConstants.DEL_REQ)) { // del
				if(key == null)
	    			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
				
				Element key = doc.createElement("Key");
	    		key.appendChild(doc.createTextNode(this.key));
	    		msg.appendChild(key);
			} else
			if(msgType.equals(KVConstants.REGISTER)) { // register
				if(message == null)
					throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
				Element message = doc.createElement("Message");
    			message.appendChild(doc.createTextNode(this.message));
    			msg.appendChild(message);
			} else
			if(msgType.equals(KVConstants.ABORT)) { // abort vote || abort decision
				if(message != null) {
					Element message = doc.createElement("Message");
	    			message.appendChild(doc.createTextNode(this.message));
	    			msg.appendChild(message);
				}
			} else
			if(msgType.equals(KVConstants.READY) // ready vote
				|| msgType.equals(KVConstants.COMMIT) // commit decision
				|| msgType.equals(KVConstants.ACK)) { // ack
				// do nothing
			} else
				// invalid request type
				throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
		}
		String xml = null;
		try {
			xml = printDoc(doc);
		} catch (Exception e) {
			throw new KVException(KVConstants.ERROR_PARSER);
		}
        return xml;
    }


    /**
     * Send serialized version of this KVMessage over the network.
     * You must call sock.shutdownOutput() in order to flush the OutputStream
     * and send an EOF (so that the receiving end knows you are done sending).
     * Do not call close on the socket. Closing a socket closes the InputStream
     * as well as the OutputStream, preventing the receipt of a response.
     *
     * @param  sock Socket to send XML through
     * @throws KVException with ERROR_INVALID_FORMAT, ERROR_PARSER, or
     *         ERROR_COULD_NOT_SEND_DATA
     */
    public void sendMessage(Socket sock) throws KVException {
		try {
			PrintWriter writer = new PrintWriter(sock.getOutputStream());
			writer.print(toXML());  
	        writer.flush();
	        // NOTE: We DO NOT close writer here! See description of sock.getOutputStream()
	        sock.shutdownOutput();
		} catch (IOException e) {
			throw new KVException(KVConstants.ERROR_COULD_NOT_SEND_DATA);
		}  
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMsgType() {
        return msgType;
    }


    @Override
    public String toString() {
        try {
            return this.toXML();
        } catch (KVException e) {
            // swallow KVException
            return e.toString();
        }
    }

    /*
     * InputStream wrapper that allows us to reuse the corresponding
     * OutputStream of the socket to send a response.
     * Please read about the problem and solution here:
     * http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html
     */
    private class NoCloseInputStream extends FilterInputStream {
        public NoCloseInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() {} // ignore close
    }

    /* http://stackoverflow.com/questions/2567416/document-to-string/2567428#2567428 */
    public static String printDoc(Document doc) {
        try {
            StringWriter sw = new StringWriter();
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            transformer.setOutputProperty(OutputKeys.METHOD, "xml");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

            transformer.transform(new DOMSource(doc), new StreamResult(sw));
            return sw.toString();
        } catch (Exception ex) {
            throw new RuntimeException("Error converting to String", ex);
        }
    }
}
