package ibis.zorilla.zoni;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;

public class ZoniOutputStream extends DataOutputStream {

    public ZoniOutputStream(OutputStream out) {
        super(out);
    }

    public void writeStringMap(Map<String,String> map) throws IOException {
        writeInt(map.size());

        for(Map.Entry<String,String> entry: map.entrySet()) {
            writeString(entry.getKey());
            writeString(entry.getValue());
        }
    }

    public void writeString(String string) throws IOException {
        if (string == null) {
            writeUTF("<<null>>");
        } else if (string.equals("<<null>>")) {
            throw new IOException("cannot write reserved string \"<<null>>\"");
        } else {
            writeUTF(string);
        }
    }

    public void writeStringArray(String[] strings) throws IOException {
        if (strings == null) {
            writeInt(-1);
            return;
        }
        
        writeInt(strings.length);

        for (int i = 0; i < strings.length; i++) {
            writeString(strings[i]);
        }
    }

    public void writeFile(File file) throws IOException {
        if (file == null) {
            writeString(null);
        } else {
            writeString(file.getPath());
        }
    }
    
//    public void writeOutputFile(ZoniOutputFile file) throws IOException {
//        writeBoolean(file == null);
//        
//        if (file == null) {
//            file.writeTo(this);
//        }
//    }
//    
//    public void writeInputFile(ZoniInputFile file, boolean writeContent) throws IOException {
//        writeBoolean(file == null);
//        
//        if (file == null) {
//            file.writeTo(this, writeContent);
//        }
//    }

    
    public void writeInetSocketAddresses(InetSocketAddress[] addresses) throws IOException {
        writeInt(addresses.length);
        for (InetSocketAddress address: addresses) {
            writeInt(address.getPort());
            byte[] bytes = address.getAddress().getAddress();
            writeInt(bytes.length);
            write(bytes);
        }
    }

}
