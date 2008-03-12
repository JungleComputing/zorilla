package ibis.zorilla.zoni;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.Logger;

public final class ZoniConnection {

    Logger logger = Logger.getLogger(ZoniConnection.class);

    private final Socket socket;

    private final ZoniInputStream in;

    private final ZoniOutputStream out;

    private final String peerID;

    public ZoniConnection(InetSocketAddress address, String id, int sourceType)
            throws IOException {
        socket = new Socket();
        socket.connect(address);

        logger.debug("connected");

        in = new ZoniInputStream(new BufferedInputStream(socket
                .getInputStream()));
        out = new ZoniOutputStream(new BufferedOutputStream(socket
                .getOutputStream()));

        out.writeInt(ZoniProtocol.VERSION);
        out.writeInt(sourceType);
        out.writeString(id);
        out.writeInt(ZoniProtocol.AUTHENTICATION_NONE);
        out.flush();

        logger.debug("send connection init");

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("connecting failed, peer replied: " + message);
        }
        peerID = in.readString();

        logger.debug("reply from peer: status = " + status + ", message = "
                + message + ", id = " + peerID);

    }

    public String peerID() {
        return peerID;
    }

    public String submitJob(JobDescription job,
            CallbackReceiver callbackReceiver) throws IOException {
        logger.debug("submitting job");

        out.writeInt(ZoniProtocol.OPCODE_SUBMIT_JOB);
        job.writeTo(out);

        if (callbackReceiver != null) {
            out.writeBoolean(true);
            out.writeInetSocketAddresses(callbackReceiver.addresses());
        } else {
            out.writeBoolean(false);
        }

        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("submission failed: " + message);
        }

        String jobID = in.readString();

        return jobID;
    }

    public JobInfo getJobInfo(String jobID) throws IOException {
        out.writeInt(ZoniProtocol.OPCODE_GET_JOB_INFO);
        out.writeString(jobID);
        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("exception on getting job info: " + message);
        }

        String id = in.readString();
        String executable = in.readString();
        Map<String, String> attributes = in.readStringMap();
        Map<String, String> jobStatus = in.readStringMap();
        int phase = in.readInt(); // phase
        int exitStatus = in.readInt();

        return new JobInfo(id, executable, attributes, jobStatus, phase,
                exitStatus);
    }

    public void setJobAttributes(String jobID,
            Map<String, String> updatedAttributes) throws IOException {

        out.writeInt(ZoniProtocol.OPCODE_SET_JOB_ATTRIBUTES);
        out.writeString(jobID);

        out.writeStringMap(updatedAttributes);

        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("exception on setting job attributes: "
                    + message);
        }
    }

    public void cancelJob(String jobID) throws IOException {
        if (jobID == null) {
            return;
        }

        logger.debug("killing job " + jobID);
        out.writeInt(ZoniProtocol.OPCODE_CANCEL_JOB);
        out.writeString(jobID);
        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("exception on cancelling job: " + message);
        }
    }

    public String[] getJobList() throws IOException {
        out.writeInt(ZoniProtocol.OPCODE_GET_JOB_LIST);
        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("exception on getting job list: " + message);
        }

        int nrOfJobs = in.readInt();

        String[] jobs = new String[nrOfJobs];

        for (int i = 0; i < nrOfJobs; i++) {
            jobs[i] = in.readString();
        }
        return jobs;
    }

    public Map getNodeInfo() throws IOException {
        logger.debug("getting node info");

        out.writeInt(ZoniProtocol.OPCODE_GET_NODE_INFO);
        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("exception on getting node info: " + message);
        }

        Map info = in.readStringMap();

        return info;
    }

    public ZoniFileInfo getFileInfo(String sandboxPath, String jobID) throws IOException {
        logger.debug("getting file info");

        out.writeInt(ZoniProtocol.OPCODE_GET_FILE_INFO);
        out.writeString(jobID);
        out.writeString(sandboxPath);
        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("exception on getting output files: " + message);
        }

        return new ZoniFileInfo(in);
    }

    /**
     * Reads standard out of the given job, and writes it to the given stream.
     * This connection is closed when this function returns.
     */
    public void getOutputStream(OutputStream stream, String jobID,
            boolean stderr) throws IOException {
        logger.debug("getting stdout/stderr");

        if (stderr) {
            out.writeInt(ZoniProtocol.OPCODE_GET_STDERR);
        } else {
            out.writeInt(ZoniProtocol.OPCODE_GET_STDOUT);
        }

        out.writeString(jobID);
        out.flush();
        
        int status = in.readInt();
        String message = in.readString();
        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("exception on getting stdout/stderr: " + message);
        }

        byte[] buffer = new byte[1024];

        while (true) {
            int read = in.read(buffer);

            if (read == -1) {
                close();
                return;
            }

            stream.write(buffer, 0, read);
            stream.flush();
        }
    }
    
    public void putInputStream(InputStream stream, String jobID) throws IOException {
        logger.debug("writing stdin");
        
        out.writeInt(ZoniProtocol.OPCODE_PUT_STDIN);
        out.writeString(jobID);
        out.flush();

        out.writeString(jobID);
        out.flush();
        
        int status = in.readInt();
        String message = in.readString();
        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("exception on putting stdin: " + message);
        }
        
        byte[] buffer = new byte[1024];

        while(true) {
            int read = stream.read(buffer);

            if (read == -1) {
                close();
                return;
            }

            out.write(buffer, 0, read);
            out.flush();
        }
    }
        

    /**
     * Writes an output file to the given stream.
     * 
     * @throws IOException
     *             in case of trouble
     */
    public void getOutputFile(OutputStream stream, String sandboxPath,
            String jobID) throws IOException {
        logger.debug("getting file");

        out.writeInt(ZoniProtocol.OPCODE_GET_FILE);

        out.writeString(jobID);
        out.writeString(sandboxPath);
        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("exception on getting output file: " + message);
        }

        long size = in.readLong();
        logger.debug("getting file " + sandboxPath + " of size " + size);
        
        byte[] buffer = new byte[1024];

        while (size > 0) {

            int tryRead = (int) Math.min(size, buffer.length);

            int read = in.read(buffer, 0, tryRead);

            if (read == -1) {
                throw new IOException("EOF on reading input file");
            }

            stream.write(buffer, 0, read);

            size = size - read;
        }

    }

    public void setNodeAttributes(Map<String, String> updatedAttributes)
            throws IOException {
        out.writeInt(ZoniProtocol.OPCODE_SET_NODE_ATTRIBUTES);

        out.writeStringMap(updatedAttributes);

        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new IOException("exception on setting peer attributes: "
                    + message);
        }

    }

    public void kill(boolean recursive) throws IOException {
        logger.debug("killing peer (recursive = " + recursive + ")");

        out.writeInt(ZoniProtocol.OPCODE_KILL_NODE);
        if (recursive) {
            out.writeInt(1);
        } else {
            out.writeInt(0);
        }
        out.flush();

        int status = in.readInt();
        String message = in.readString();

        // kill always closes the connecton
        close();

        if (status != ZoniProtocol.STATUS_OK) {
            throw new IOException("exception on killing peer: " + message);
        }
    }

    public void close() {
        if (!socket.isClosed()) {
            try {
                out.writeInt(ZoniProtocol.OPCODE_CLOSE_CONNECTION);
                out.flush();
            } catch (IOException e) {
                logger.debug("IOException on closing connection", e);
            }
            try {
                socket.close();
            } catch (IOException e) {
                logger.debug("IOException on closing socket", e);
            }
        }
    }

    public boolean isClosed() {
        return socket.isClosed();
    }
}
