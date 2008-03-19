package ibis.zorilla.job;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePortIdentifier;
import ibis.zorilla.Node;
import ibis.zorilla.io.Hash;
import ibis.zorilla.io.ObjectInput;
import ibis.zorilla.io.ObjectOutput;
import ibis.zorilla.job.net.Call;
import ibis.zorilla.job.net.EndPoint;
import ibis.zorilla.job.net.Invocation;
import ibis.zorilla.job.net.Receiver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.log4j.Logger;

public class InputFile implements Receiver {

    public static final int DOWNLOAD_ATTEMPTS = 1;
    public static final int PRIMARY_DOWNLOAD_TRESHOLD = 2;

    private static final int BUFFER_SIZE = 31000;

    private static final Logger logger = Logger.getLogger(InputFile.class);

    private final String sandboxPath;

    private final File file;

    private final UUID id;

    private final EndPoint endPoint;

    private final Job job;

    private final long size;

    private final Hash hash;

    private final ReceivePortIdentifier primary;

    private boolean downloaded;

    // create a primary input file
    public InputFile(File file, String sandboxPath, Primary p)
            throws Exception, IOException {
        this.file = file;
        this.sandboxPath = sandboxPath;
        this.id = Node.generateUUID();
        this.job = p;

        downloaded = true;

        if (sandboxPath.startsWith("/")) {
            throw new Exception("File sandbox path (" + sandboxPath
                    + ") cannot start with a \"\\\"");
        }

        if (!file.isAbsolute()) {
            throw new Exception("File (" + file + ") not absolute");
        }

        if (file.isDirectory()) {
            throw new Exception("File (" + file + ") is a directory");
        }

        if (!file.canRead()) {
            throw new Exception("cannot read from file (" + file + ")");
        }

        logger.debug("new input file: " + file + " sandbox path = "
                + sandboxPath);

        size = file.length();

        // calculate hash for this file
        hash = new Hash(file);

        endPoint = job.newEndPoint(id.toString(), this);

        primary = endPoint.getID();
    }

    // create a copy input file
    public InputFile(ObjectInput in, Copy copy, File tmpDir)
            throws IOException, Exception {

        this.job = copy;

        downloaded = false;

        try {
            sandboxPath = in.readString();
            id = (UUID) in.readObject();
            size = in.readLong();
            primary = (ReceivePortIdentifier) in.readObject();
            hash = (Hash) in.readObject();

        } catch (ClassNotFoundException e) {
            throw new Exception("could not read bootstrap", e);
        }

        file = File.createTempFile("zorilla", ".input", tmpDir);

        endPoint = job.newEndPoint(id.toString(), this);
    }

    public long size() {
        return size;
    }

    public void writeBootStrap(ObjectOutput output) throws IOException {
        output.writeString(sandboxPath);
        output.writeObject(id);
        output.writeLong(size);
        output.writeObject(primary);
        output.writeObject(hash);
    }

    public String sandboxPath() {
        return sandboxPath;
    }

    public UUID id() {
        return id;
    }

    public File copyTo(File dir) throws Exception {

        logger.debug("copying " + file + " to " + dir);
        
        if (!isDownloaded()) {
            throw new Exception("file not downloaded");
        }

        if (sandboxPath == null) {
            throw new Exception("cannot copy input file without virtual path");
        }

        File destFile = new File(dir.getAbsolutePath() + File.separator
                + sandboxPath);
        destFile.getParentFile().mkdirs();

        if (!destFile.getParentFile().isDirectory()) {
            throw new Exception("could not create directory: "
                    + destFile.getParentFile());
        }

        logger.debug("destination file = " + destFile);

        try {

            byte[] buffer = new byte[BUFFER_SIZE];

            FileInputStream in = new FileInputStream(file);
            FileOutputStream out = new FileOutputStream(destFile);

            while (true) {
                int read = in.read(buffer);

                if (read == -1) {
                    logger.debug("done copying file " + destFile);
                    return destFile;
                }

                out.write(buffer, 0, read);
            }

        } catch (IOException e) {
            throw new Exception("could not copy file", e);
        }

    }

    public void receive(ReadMessage message) {
        job.log("message received in file", new Exception(
                "message received in file"));
    }

    private void readFrom(ObjectInput in) throws IOException {
        logger.debug("reading " + sandboxPath);

        byte[] buffer = new byte[BUFFER_SIZE];

        FileOutputStream out = new FileOutputStream(file);

        while (true) {
            int size = in.readInt();
            
            if (size == -1) {
                // EOF
                out.flush();
                out.close();
                return;
            }

            in.readArray(buffer, 0, size);
            
            out.write(buffer, 0, size);
        }
    }

    private void writeTo(Invocation invocation) throws IOException {
        logger.debug("writing " + sandboxPath + " from " + file);
                        

        logger.debug("checking current value of hash of " + sandboxPath);
        Hash hash = new Hash(file);

        if (!hash.equals(this.hash)) {
            throw new IOException("current value of hash not equal to initial hash");
        }

        logger.debug("done checking hash, writing file "
                + sandboxPath + " with hash " + hash);


        byte[] buffer = new byte[BUFFER_SIZE];

        FileInputStream in = new FileInputStream(file);
        
        if (in.available() != size) {
            logger.error("file size " + in.available() + " not equal to " + size);
        }

        while (true) {
            int read = in.read(buffer);

            if (read == -1) {
                // EOF
                invocation.writeInt(-1);
                in.close();
                return;
            }
            
            invocation.writeInt(read);
            invocation.writeArray(buffer, 0, read);
            invocation.flush();
        }
    }

    private synchronized boolean isDownloaded() {
        return downloaded;
    }

    private synchronized void setDownloaded() {
        downloaded = true;
    }

    protected void download() throws Exception {
        if (isDownloaded()) {
            return;
        }

        logger.debug("dowloading " + sandboxPath);

        int triesLeft = DOWNLOAD_ATTEMPTS;

        while (triesLeft > 0) {
            Call call;

            try {
                if (triesLeft < PRIMARY_DOWNLOAD_TRESHOLD) {
                    call = endPoint.call(primary);
                } else {
                    IbisIdentifier victim = job.getRandomConstituent();
                    call = endPoint.call(victim, id().toString());
                }

                logger.debug("written request, waiting for reply");

                call.call();

                boolean ok = call.readBoolean();

                if (!ok) {
                    logger.debug("file not available at peer, trying another");
                    call.finish();
                    triesLeft--;
                    continue;
                }

                // delete existing file (if any)
                file.delete();

                readFrom(call);

                Hash hash = new Hash(file);

                if (hash.equals(this.hash)) {
                    logger.debug("done downloading " + sandboxPath);
                    setDownloaded();
                    return;
                } else {
                    logger.warn("hash does not match, trying again");
                    triesLeft--;
                }
            } catch (Exception e) {
                job.log("exeption on downloading file", e);
                triesLeft--;
            }
        }
        throw new Exception("could not download file " + sandboxPath + " to " + file);
    }

    public void invoke(Invocation invocation) throws Exception, IOException {

        //nothing to read
        invocation.finishRead();

        if (!isDownloaded()) {
            //we don't have the file ourselves
            invocation.writeBoolean(false);
            invocation.finish();
            return;
        }

        invocation.writeBoolean(true);

        writeTo(invocation);

        logger.debug("done writing input");
        invocation.finish();
    }

    public void close() throws Exception {
        try {
            endPoint.close();
        } catch (IOException e) {
            throw new Exception("could not close endpoint", e);
        }
    }

    public String toString() {
        return sandboxPath;
    }

}
