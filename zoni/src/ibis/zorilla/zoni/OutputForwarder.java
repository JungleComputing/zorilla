package ibis.zorilla.zoni;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class OutputForwarder extends Thread {

    private final ZoniConnection connection;

    private final String jobID;
    
    private final OutputStream stream;

    private final boolean isStderr;

    public OutputForwarder(InetSocketAddress nodeAddress, String jobID, OutputStream stream,
            boolean isStderr ) throws IOException {
        connection = new ZoniConnection(nodeAddress, null,
                ZoniProtocol.TYPE_CLIENT);
        
        this.jobID = jobID;
        this.stream = stream;
        this.isStderr = isStderr;
    }
    
    public void startAsDaemon() {
        this.setDaemon(true);
        this.start();
    }

    public void run() {
        try {
            connection.getOutputStream(stream, jobID, isStderr);
        } catch (IOException e) {
            System.err.println("error in getting output for " + jobID + ": "
                    + e.getMessage());
        }
    }

}
