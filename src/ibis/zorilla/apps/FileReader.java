package ibis.zorilla.apps;

import java.io.File;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import org.apache.log4j.Logger;

/**
 * @author Niels Drost
 * 
 * reads data from a stream and writes it to and outputstreams. Assumes a file
 * is growing.
 */
public final class FileReader implements Runnable {

    static final Logger logger = Logger.getLogger(FileReader.class);

    private final File file;

    private final OutputStream out;

    private boolean ended = false;

    public FileReader(File file, OutputStream out) {

        this.file = file;
        
        try {
            file.createNewFile();
        } catch (IOException e) {
            System.err.println("error on creating file: " + file + "error = " + e);
            System.exit(1);
        }
        
        this.out = out;

        logger.debug("created file reader");

        new Thread(this).start();
    }

    public synchronized void end() throws IOException {
        ended = true;
        notifyAll();
    }

    public void run() {
        long offset = 0;
        byte[] data = new byte[10 * 1024];
        RandomAccessFile in = null;

        if (file == null) {
            return;
        }

        try {
            while (true) {
                synchronized(this) {
                    if (ended) {
                        return;
                    }
                }
                        

                if (in == null) {
                    if (file.exists()) {
                        in = new RandomAccessFile(file, "r");
                    }
                }

                if (in != null && in.length() > offset) {

                    logger.debug("reading from file at offset " + offset);
                    
                    in.seek(offset);

                    int bytesRead = in.read(data, 0, data.length);

                    logger.debug("read " + bytesRead + " bytes");

                    out.write(data, 0, bytesRead);
                    offset += bytesRead;
                } else {
                    Thread.sleep(500);
                }
            }
        } catch (Exception e) {
            logger.warn("exception in reading from file: " + e);
        }
    }
}