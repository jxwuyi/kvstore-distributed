package kvstore;

import static kvstore.KVConstants.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * This is a basic key-value store. Ideally this would go to disk, or some other
 * backing store.
 */
public class KVStore implements KeyValueInterface {

    private ConcurrentHashMap<String, String> store;

    /**
     * Construct a new KVStore.
     */
    public KVStore() {
        resetStore();
    }

    private void resetStore() {
        this.store = new ConcurrentHashMap<String, String>();
    }

    /**
     * Insert key, value pair into the store.
     *
     * @param  key String key
     * @param  value String value
     */
    @Override
    public void put(String key, String value) {
        store.put(key, value);
    }

    /**
     * Retrieve the value corresponding to the provided key
     * @param  key String key
     * @throws KVException with ERROR_NO_SUCH_KEY if key does not exist in store
     */
    @Override
    public String get(String key) throws KVException {
        String retVal = this.store.get(key);
        if (retVal == null) {
            KVMessage msg = new KVMessage(KVConstants.RESP, ERROR_NO_SUCH_KEY);
            throw new KVException(msg);
        }
        return retVal;
    }

    /**
     * Delete the value corresponding to the provided key.
     *
     * @param  key String key
     * @throws KVException with ERROR_NO_SUCH_KEY if key does not exist in store
     */
    @Override
    public void del(String key) throws KVException {
        if(key != null) {
            if (!this.store.containsKey(key)) {
                KVMessage msg = new KVMessage(KVConstants.RESP, ERROR_NO_SUCH_KEY);
                throw new KVException(msg);
            }
            this.store.remove(key);
        }
    }

    /**
     * Serialize the store to XML. See the spec for specific output format.
     * This method is best effort. Any exceptions that arise can be dropped.
     */
    public String toXML() {
    	try {
    		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
    		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
    		Document doc = docBuilder.newDocument();
    		Element rootElement = doc.createElement("KVStore");
    		doc.appendChild(rootElement);
    		
    		for(Entry<String, String> e: store.entrySet()) {
    			Element pair = doc.createElement("KVPair");
    			rootElement.appendChild(pair);
    			
    			Element key = doc.createElement("Key");
    			key.appendChild(doc.createTextNode(e.getKey()));
    			Element value = doc.createElement("Value");
    			value.appendChild(doc.createTextNode(e.getValue()));
    			
    			pair.appendChild(key);
    			pair.appendChild(value);
    		}
    		// TODO: probably use a better transformer
    		return KVMessage.printDoc(doc);
		} catch (Exception e) {// best effort
			return null;
		}
    }

    @Override
    public String toString() {
        return this.toXML();
    }

    /**
     * Serialize to XML and write the output to a file.
     * This method is best effort. Any exceptions that arise can be dropped.
     *
     * @param fileName the file to write the serialized store
     */
    public void dumpToFile(String fileName) {
    	try {
			PrintWriter wt = new PrintWriter(new FileWriter(fileName));
			wt.print(this.toXML());
			wt.close();
		} catch (Exception e) {
			return ;
		} 
    }

    /**
     * Replaces the contents of the store with the contents of a file
     * written by dumpToFile; the previous contents of the store are lost.
     * The store is cleared even if the file does not exist.
     * This method is best effort. Any exceptions that arise can be dropped.
     *
     * @param fileName the file containing the serialized store data
     */
    public void restoreFromFile(String fileName) {
        resetStore();
       
		try {
			File xmlFile = new File(fileName);
	    	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	    	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	    	// use default errorHandler
	    	dBuilder.setErrorHandler(null);
	    	
	    	Document doc = dBuilder.parse(xmlFile);
	    	Element root = doc.getDocumentElement();
	    	
	    	root.normalize(); // normalization is recommended
	    	
	    	NodeList lst = root.getElementsByTagName("KVPair");
	    	for(int i=0;i<lst.getLength();++i) {
	    		Node node = lst.item(i);
	    		if(node.getNodeType() == Node.ELEMENT_NODE) {
	    			Element elem = (Element) node;
	    			
	    			String key = elem.getElementsByTagName("Key").item(0).getTextContent();
	    			String value = elem.getElementsByTagName("Value").item(0).getTextContent();
	    			store.put(key, value);
	    		}
	    	}
		} catch (Exception e) { // ignore any exception
			return ;
		}
    }
}
