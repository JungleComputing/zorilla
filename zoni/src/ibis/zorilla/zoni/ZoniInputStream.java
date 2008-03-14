package ibis.zorilla.zoni;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class ZoniInputStream extends DataInputStream {

    public ZoniInputStream(InputStream in) {
        super(in);
    }

    public Map<String, String> readStringMap() throws IOException {
        Map<String, String> result = new HashMap<String, String>();

        int nrOfEntries = readInt();

        for (int i = 0; i < nrOfEntries; i++) {
            result.put(readString(), readString());
        }

        return result;
    }

    public String readString() throws IOException {
        String result = readUTF();

        if (result.equals("<<null>>")) {
            return null;
        }
        return result;
    }

    public String[] readStringArray() throws IOException {
        int length = readInt();
        
        if (length == -1) {
            return null;
        }
        
        String[] result = new String[length];

        for (int i = 0; i < length; i++) {
            result[i] = readString();
        }

        return result;
    }

    public InetSocketAddress[] readInetSocketAddresses() throws IOException {
        InetSocketAddress[] result = new InetSocketAddress[readInt()];

        for (int i = 0; i < result.length; i++) {
            int port = readInt();
            int byteSize = readInt();
            byte[] bytes = new byte[byteSize];

            readFully(bytes);

            result[i] = new InetSocketAddress(InetAddress.getByAddress(bytes),
                    port);
        }

        return result;
    }

    public File readFile() throws IOException {
        String string = readString();

        if (string == null) {
            return null;
        }

        return new File(string);
    }

}
