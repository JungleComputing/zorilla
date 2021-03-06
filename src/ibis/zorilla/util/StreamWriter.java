package ibis.zorilla.util;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

/**
 * @author Niels Drost
 * 
 *         reads data from a stream and writes it to one or more OutputStreams.
 */
public final class StreamWriter extends Thread {

    static final Logger logger = Logger.getLogger(StreamWriter.class);

    public static final int BUFFER_SIZE = 100 * 1024;

    private final InputStream in;

    private final OutputStream[] outputs;

    private boolean done = false;

    public StreamWriter(InputStream in, OutputStream[] outputs) {
        this.in = in;
        this.outputs = outputs.clone();

        logger.debug("created stream writer");

        setName("stream writer");
        setDaemon(true);
        start();
    }

    public StreamWriter(InputStream in, OutputStream out) {
        this(in, new OutputStream[] { out });
    }

    public void run() {
        byte[] data = new byte[BUFFER_SIZE];
        int bytesRead;

        try {
            while (true) {
                bytesRead = in.read(data);

                if (bytesRead == -1) {
                    synchronized (this) {
                        done = true;
                        notifyAll();
                    }
                    return;
                }
                
                if (bytesRead == 0) {
                    logger.error("Read 0 bytes from stream: " + in  + ", Assuming EOS");
                    synchronized (this) {
                        done = true;
                        notifyAll();
                    }
                    return;
                }
                
                for (int i = 0; i < outputs.length; i++) {
                    if (outputs[i] != null) {
                        outputs[i].write(data, 0, bytesRead);
                    }
                }

            }
        } catch (Exception e) {
            synchronized (this) {
                done = true;
                notifyAll();
            }
        }
    }

    public synchronized void waitFor() {
        while (!done) {
            try {
                wait();
            } catch (Exception e) {
                // IGNORE
            }
        }
    }

}