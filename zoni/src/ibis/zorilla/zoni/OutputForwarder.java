package ibis.zorilla.zoni;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.log4j.Logger;

public class OutputForwarder extends Thread {

    private static final Logger logger = Logger
            .getLogger(OutputForwarder.class);

    private final ZoniConnection connection;

    private final String jobID;

    private final OutputStream stream;

    private final boolean isStderr;

    public OutputForwarder(String nodeAddress, String jobID,
            OutputStream stream, boolean isStderr) throws IOException {
        connection = new ZoniConnection(nodeAddress, null);

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
            logger.warn("error in getting output for " + jobID, e);
        }
    }

}
