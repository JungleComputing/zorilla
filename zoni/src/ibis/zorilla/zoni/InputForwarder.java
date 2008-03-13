package ibis.zorilla.zoni;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;

public class InputForwarder extends Thread {

    private final ZoniConnection connection;

    private final String jobID;
    
    private final InputStream stream;

    public InputForwarder(InetSocketAddress nodeAddress, String jobID, InputStream stream) throws IOException {
        connection = new ZoniConnection(nodeAddress, null,
                ZoniProtocol.TYPE_CLIENT);
        
        this.jobID = jobID;
        this.stream = stream;
    }
    
    public void startAsDaemon() {
        this.setDaemon(true);
        this.start();
    }

    public void run() {
        try {
            connection.putInputStream(stream, jobID);
        } catch (IOException e) {
            System.err.println("error while forwarding stdin to job " + jobID + ": "
                    + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
    
    public void close() {
        try {
            stream.close();
        } catch (IOException e) {
            //IGNORE
        }
    }

}
