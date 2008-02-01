package nl.vu.zorilla.util;

import ibis.util.ThreadPool;

import java.io.InputStream;
import java.io.OutputStream;

import nl.vu.zorilla.Config;

import org.apache.log4j.Logger;

/**
 * @author Niels Drost
 * 
 * reads data from a stream and writes it to one or more OutputStreams.
 */
public final class StreamWriter implements Runnable {

    static final Logger logger = Logger.getLogger(StreamWriter.class);

    private final InputStream in;

    private final OutputStream[] outputs;

    private boolean done = false;

    public StreamWriter(InputStream in, OutputStream[] outputs) {
        this.in = in;
        this.outputs = outputs.clone();

        logger.debug("created stream writer");

        ThreadPool.createNew(this, "stream writer");
    }

    public StreamWriter(InputStream in, OutputStream out) {
        this(in, new OutputStream[] { out });
    }

    public void run() {
        byte[] data = new byte[Config.BUFFER_SIZE];
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