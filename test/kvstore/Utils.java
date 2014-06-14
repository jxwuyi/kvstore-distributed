package kvstore;

public class Utils {

    static boolean containsMsg(Exception e, String msg) {
        return (((KVException) e).getKVMessage().getMessage()).toLowerCase()
                .contains(msg.toLowerCase());
    }

    static String makeLongString(int n) {
        return new String(new char[n]).replace('\0', 'a');
    }

    static String truncateString(String s) {
        if (s == null) {
            return "<null>";
        }
        int n = s.length();
        if (n > 20) {
            return String.format("%s...", s.substring(0, 20));
        } else {
            return s;
        }
    }

}
