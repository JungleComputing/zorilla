package ibis.zorilla.job;

import ibis.ipl.ReadMessage;
import ibis.zorilla.Node;
import ibis.zorilla.io.ObjectOutput;
import ibis.zorilla.job.net.EndPoint;
import ibis.zorilla.job.net.Invocation;
import ibis.zorilla.job.net.Receiver;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.log4j.Logger;

final class PrimaryOutputStream extends OutputStream implements Receiver {

    public static final int BUFFER_SIZE = 100 * 1024;

    static final int WRITE_BLOCK = 0;

    private static final Logger logger = Logger
            .getLogger(PrimaryOutputStream.class);

    // private static final Logger logger = Logger
    // .getLogger(PrimaryOutputFile.class);

    private final String sandboxPath;

    private final UUID id;

    private final File file;

    private final Primary primary;

    private final EndPoint endPoint;

    private boolean done = false;

    private long size = 0;

    /**
     * Creates a stream which appends to the given file.
     * 
     * @throws Exception
     *             if the outputfile could not be exported
     * @throws IOException
     */
    PrimaryOutputStream(File file, Primary primary, boolean export)
            throws Exception, IOException {
        logger.debug("creating log file (" + file + " at primary");

        this.file = file;

        this.primary = primary;
        sandboxPath = "<log file>";

        id = Node.generateUUID();

        if (export) {
            endPoint = primary.newEndPoint(id.toString(), this);
        } else {
            endPoint = null;
        }
    }

    /**
     * Creates a stream representing either an output file or an output stream.
     * This stream is part of the "virtual file system" of zorilla.
     * 
     * @throws IOException
     */
    public PrimaryOutputStream(String sandboxPath, File file, Primary primary)
            throws Exception, IOException {
        this.sandboxPath = sandboxPath;
        this.primary = primary;
        this.file = file;

        if (sandboxPath == null) {
            throw new Exception("sandbox path cannot be null");
        }

        if (file == null) {
            throw new Exception("file cannot be null");
        }

        if (!file.isAbsolute()) {
            throw new Exception("file must be absolute: " + file);
        }

        if (file.isDirectory()) {
            throw new Exception("File (" + file + ") is a directory");
        }

        file.delete();

        if (file.exists()) {
            throw new Exception("could not purge output file: " + file);
        }

        // (re) create file
        file.createNewFile();

        if (!file.canWrite()) {
            throw new Exception("cannot write to file: " + file);
        }

        logger.debug("creating primary output file for path " + sandboxPath
                + " with backing file " + file);

        id = Node.generateUUID();

        endPoint = primary.newEndPoint(id.toString(), this);
    }

    public String sandboxPath() {
        return sandboxPath;
    }

    @Override
    public synchronized void write(byte[] data, int offset, int length)
            throws IOException {
        logger.debug("writing to " + sandboxPath + " stored in " + file);

        FileOutputStream out = new FileOutputStream(file, true);

        out.write(data, offset, length);
        out.flush();

        out.close();

        size += length;

        if (file.length() != size) {
            logger.warn("write: warning: file of length " + file.length()
                    + " should be of size " + size);
        }

        notifyAll();
    }

    @Override
    public synchronized void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public synchronized void write(int b) throws IOException {

        byte[] byteArray = new byte[1];

        byteArray[0] = (byte) b;

        write(byteArray, 0, byteArray.length);
    }

    @Override
    public void flush() throws IOException {
        // NOTHING
    }

    @Override
    public void close() {
        // NOTHING (could be called by user)
    }

    // the _real_ close
    synchronized void unexport() throws Exception {
        done = true;
        notifyAll();

        if (endPoint != null) {
            try {
                endPoint.close();
            } catch (IOException e) {
                throw new Exception("exception closing endpoint", e);
            }
        }
        // NOTHING
    }

    public void writeBootStrap(ObjectOutput output) throws IOException {
        if (endPoint == null) {
            throw new IOException("file not exported");
        }
        output.writeString(sandboxPath());
        output.writeObject(id);
        output.writeObject(endPoint.getID());
    }

    public void receive(Message message) {
        try {
            logger.debug("writing to file from message");

            byte[] buffer = new byte[BUFFER_SIZE];

            while (true) {
                int length = message.readInt();

                if (length == -1) {
                    // done
                    message.finish();
                    return;
                }

                message.readArray(buffer, 0, length);
                write(buffer, 0, length);
            }
        } catch (Exception e) {
            primary.log("error on reading output from message", e);
        }
    }

    public void invoke(Invocation invocation) throws Exception, IOException {
        throw new Exception("cannot invoke primary output file");

    }

    void readFrom(InputStream data) throws IOException {
        // stream data to other side
        byte[] buffer = new byte[BUFFER_SIZE];
        while (true) {
            int read = data.read(buffer);

            if (read == -1) {
                data.close();
                logger.debug("writing done");
                return;
            }

            write(buffer, 0, read);
        }
    }

    public String toString() {
        return "sandbox path = " + sandboxPath + ", file = " + file;
    }

    // fetch data from the output file
    private synchronized int getData(long offset, byte[] buffer)
            throws IOException {
        logger.debug("getting data");

        while (true) {
            FileInputStream in = new FileInputStream(file);

            if (in.skip(offset) == offset && in.available() > 0) {
                int result = in.read(buffer);

                in.close();

                return result;
            }

            in.close();

            if (done) {
                logger.debug("no more data");
                // no more data
                return -1;
            }

            try {
                wait();
            } catch (InterruptedException e) {
                // IGNORE
            }

        }

    }

    public void streamTo(OutputStream out) throws IOException {
        long offset = 0;
        byte[] buffer = new byte[1024];

        synchronized (this) {
            if (file.length() != size) {
                logger.warn("streamto: warning: file of length "
                        + file.length() + " should be of size " + size);
            }
        }

        while (true) {
            int read = getData(offset, buffer);

            if (read == -1) {
                // EOF :)
                logger.debug("done streaming file, streamed " + offset
                        + " bytes");
                return;
            }

            out.write(buffer, 0, read);
            out.flush();

            offset = offset + read;
        }

    }

    public synchronized long length() throws IOException {
        if (file.length() != size) {
            logger.warn("length(): warning: file " + file + " of length "
                    + file.length() + " should be of size " + size);
        }

        return size;
    }

}
