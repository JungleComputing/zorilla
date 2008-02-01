package ibis.zorilla.util;

import ibis.util.ThreadPool;
import ibis.zorilla.io.InputFile;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;


/**
 * @author Niels Drost
 * 
 * reads data from a stream and writes it to one or more FileOutputStreams.
 */
public final class FileReader implements Runnable {

    static final Logger logger = Logger.getLogger(FileReader.class);

    public static final int BUFFER_SIZE = 100 * 1024;

    private final InputFile in;

    private final OutputStream out;

    private boolean done = false;

    public FileReader(InputFile in, OutputStream out) {
        this.in = in;
        this.out = out;

        logger.debug("created file reader");

        ThreadPool.createNew(this, "file reader");
    }

    public void close() throws IOException {
        if (out != null) {
            out.close();
        }
    }

    private synchronized void done() {
        done = true;
        try {
            out.close();
        } catch (IOException e) {
            logger.warn("exception on closing stream", e);
        }
        notifyAll();
    }

    public void run() {
        long offset = 0;
        byte[] data = new byte[BUFFER_SIZE];
        int bytesRead;

        if (in == null) {
            done();
            return;
        }

        try {
            while (true) {

                logger.debug("reading from file at offset " + offset);

                bytesRead = in.read(offset, data, 0, data.length);

                logger.debug("read " + bytesRead + " bytes");

                if (bytesRead == -1) {
                    logger.debug("end of file reached, closing output stream");
                    done();
                    return;
                }

                out.write(data, 0, bytesRead);
                offset += bytesRead;

            }
        } catch (Exception e) {
            logger.warn("exception in reading from file: " + e);
            done();
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