package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Data structure to maintain information about SlaveServers
 */
public class SlaveInfo {
    // 64-bit globally unique ID of the SlaveServer
    public long slaveID = -1;
    // Name of the host this SlaveServer is running on
    public String hostName = null;
    // Port which SlaveServer is listening to
    public int port = -1;
    // Regex to parse slave info
    private static final Pattern SLAVE_INFO_REGEX = Pattern.compile("^(.*)@(.*):(.*)$");
    // Timeout value used during 2PC operations
    public static final int TIMEOUT_MILLISECONDS = 2000;

    /**
     *
     * @param slaveInfo as "SlaveServerID@HostName:Port"
     * @throws KVException
     */
    public SlaveInfo(String slaveInfo) throws KVException {
        try {
            Matcher slaveInfoMatcher = SLAVE_INFO_REGEX.matcher(slaveInfo);

            if (!slaveInfoMatcher.matches()) {
                throw new IllegalArgumentException();
            }

            slaveID = Long.parseLong(slaveInfoMatcher.group(1));
            hostName = slaveInfoMatcher.group(2);
            port = Integer.parseInt(slaveInfoMatcher.group(3));
        } catch (Exception ex) {
            throw new KVException(new KVMessage(
                RESP, "Unknown Error: Could not parse slave info"));
        }
    }


    public long getSlaveID() {
        return slaveID;
    }

    public Socket connectHost() throws KVException {
        try {
            Socket sock = new Socket();
            sock.setSoTimeout(TIMEOUT_MILLISECONDS);
            sock.connect(new InetSocketAddress(hostName, port), TIMEOUT_MILLISECONDS);
            return sock;
        } catch (UnknownHostException ex) {
            throw new KVException(new KVMessage(
                RESP, "Network Error: Could not connect"));
        } catch (IOException ex) {
            throw new KVException(new KVMessage(
                RESP, "Network Error: Could not create socket"));
        }
    }


    public void closeHost(Socket sock) throws KVException {
        try {
            sock.close();
        } catch (IOException ex) {
            throw new KVException(new KVMessage(
                RESP, "Network Error: Could not close socket"));
        }
    }
}