package nl.vu.zorilla.zoni;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ZoniInputStream extends DataInputStream {

    public ZoniInputStream(InputStream in) {
        super(in);
    }

    public Map readStringMap() throws IOException {
        Map result = new HashMap();

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
    
    public String[] readStrings() throws IOException {
        String[] result = new String[readInt()];
        
        for (int i = 0; i < result.length; i++) {
            result[i] = readString();
        }
        
        return result;
    }
}
