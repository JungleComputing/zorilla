package nl.vu.zorilla.job.primaryCopy;

import ibis.ipl.ReadMessage;

import java.io.File;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.log4j.Logger;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.io.ObjectOutput;
import nl.vu.zorilla.job.net.EndPoint;
import nl.vu.zorilla.job.net.Invocation;
import nl.vu.zorilla.job.net.Receiver;

final class PrimaryOutputStream extends OutputStream implements Receiver {
    
    public static final int BUFFER_SIZE = 100 * 1024;

    static final int WRITE_BLOCK = 0;

    private static final Logger logger = Logger
            .getLogger(PrimaryOutputStream.class);

    // private static final Logger logger = Logger
    // .getLogger(PrimaryOutputFile.class);

    private final String virtualPath;

    private final UUID id;

    private final File file;

    private final Primary primary;

    private final EndPoint endPoint;

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
        virtualPath = null;

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
    public PrimaryOutputStream(String virtualPath, String physicalPath,
            Primary primary, boolean stream) throws Exception,
            IOException {

        logger.debug("creating primary output file for path " + virtualPath
                + " with backing file " + physicalPath);

        if (virtualPath == null) {
            throw new Exception("virtual path cannot be null");
        }

        if (!(virtualPath.startsWith("/") || virtualPath.equals("<stderr>") || virtualPath
                .equals("<stdout>"))) {
            throw new Exception("virtual path must be absolute");
        }

        this.virtualPath = virtualPath;
        this.primary = primary;

        file = createFile(physicalPath);

        id = Node.generateUUID();

        endPoint = primary.newEndPoint(id.toString(), this);
    }

    private static File createFile(String path) throws Exception,
            IOException {
        File file = new File(path);

        if (!file.isAbsolute()) {
            throw new Exception("cannot handle non-absolute file: "
                    + file);
        }

        if (file.isDirectory()) {
            throw new Exception("cannot write output to directory: "
                    + file);
        }

        file.delete();

        if (file.exists()) {
            throw new Exception("could not perge output file");
        }

        // (re) create file
        file.createNewFile();

        if (!file.canWrite()) {
            throw new Exception("cannot write to file");
        }

        return file;
    }

    public String path() {
        return virtualPath;
    }

    @Override
    public synchronized void write(byte[] data, int offset, int length)
            throws IOException {
        logger.debug("writing to " + virtualPath + " stored in " + file);

        FileOutputStream out = new FileOutputStream(file, true);

        out.write(data, offset, length);

        out.close();
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
    void unexport() throws Exception {
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
        output.writeString(virtualPath);
        output.writeObject(id);
        output.writeObject(endPoint.getID());
    }

    public void receive(ReadMessage message) {
        try {
            logger.debug("writing to file from message");

            OutputStream out;

            // append to local file
            out = new FileOutputStream(file, true);

            byte[] buffer = new byte[BUFFER_SIZE];

            while (true) {
                int length = message.readInt();

                if (length == -1) {
                    // done
                    message.finish();
                    out.close();
                    return;
                }

                message.readArray(buffer, 0, length);
                out.write(buffer, 0, length);
            }
        } catch (Exception e) {
            primary.log("error on reading output from message", e);
        }
    }

    public void invoke(Invocation invocation) throws Exception,
            IOException {
        throw new Exception("cannot invoke primary output file");

    }

    synchronized void readFrom(InputStream data) throws IOException {
        OutputStream out = new FileOutputStream(file, true);

        // stream data to other side
        byte[] buffer = new byte[BUFFER_SIZE];
        while (true) {
            int read = data.read(buffer);

            if (read == -1) {
                data.close();
                out.close();
                logger.debug("writing done");
                return;
            }

            out.write(buffer, 0, read);
        }
    }
    
    public String toString() {
        return virtualPath + "=" + file;
    }

}
