package nl.vu.zorilla.zoni;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
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
            throws IOException, ZoniException {
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
            throw new ZoniException("connecting failed, peer replied: "
                    + message);
        }
        peerID = in.readString();

        logger.debug("reply from peer: status = " + status + ", message = "
                + message + ", id = " + peerID);

    }

    public String peerID() {
        return peerID;
    }

    public String submitJob(String executable, String[] arguments,
            Map<String,String> environment, Map<String,String> attributes, Map<String,String> preStage, Map<String,String> postStage,
            String stdinPath, String stdoutPath, String stderrPath)
            throws ZoniException, IOException {
        logger.debug("submitting job");

        out.writeInt(ZoniProtocol.OPCODE_SUBMIT_JOB);
        out.writeString(executable);

        // arguments;
        out.writeStrings(arguments);

        logger.debug("writing environment:");
        out.writeStringMap(environment);
        logger.debug("writing attributes");
        out.writeStringMap(attributes);

        logger.debug("writing pre stage:");
        out.writeStringMap(preStage);
        logger.debug("writing post stage:");
        out.writeStringMap(postStage);

        out.writeString(stdinPath);
        out.writeString(stdoutPath);
        out.writeString(stderrPath);

        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new ZoniException("exception on submitting job: " + message);
        }

        String jobID = in.readString();

        return jobID;
    }

    public JobInfo getJobInfo(String jobID) throws ZoniException, IOException {
        out.writeInt(ZoniProtocol.OPCODE_GET_JOB_INFO);
        out.writeString(jobID);
        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new ZoniException("exception on getting job info: " + message);
        }

        String id = in.readString();
        String executable = in.readString();
        Map<String,String> attributes = in.readStringMap();
        Map<String,String> jobStatus = in.readStringMap();
        int phase = in.readInt(); // phase

        return new JobInfo(id, executable, attributes, jobStatus, phase);
    }

    public void setJobAttributes(String jobID, Map<String,String> updatedAttributes)
            throws ZoniException, IOException {

        out.writeInt(ZoniProtocol.OPCODE_SET_JOB_ATTRIBUTES);
        out.writeString(jobID);

        out.writeStringMap(updatedAttributes);

        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new ZoniException("exception on setting job attributes: "
                    + message);
        }
    }

    public void cancelJob(String jobID) throws ZoniException, IOException {
        logger.debug("killing job " + jobID);
        out.writeInt(ZoniProtocol.OPCODE_CANCEL_JOB);
        out.writeString(jobID);
        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new ZoniException("exception on cancelling job: " + message);
        }
    }

    public String[] getJobList() throws ZoniException, IOException {
        out.writeInt(ZoniProtocol.OPCODE_GET_JOB_LIST);
        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new ZoniException("exception on getting job list: " + message);
        }

        int nrOfJobs = in.readInt();

        String[] jobs = new String[nrOfJobs];

        for (int i = 0; i < nrOfJobs; i++) {
            jobs[i] = in.readString();
        }
        return jobs;
    }

    public Map getNodeInfo() throws ZoniException, IOException {
        logger.debug("getting peer info");

        out.writeInt(ZoniProtocol.OPCODE_GET_NODE_INFO);
        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new ZoniException("exception on getting peer info: "
                    + message);
        }

        Map info = in.readStringMap();

        return info;
    }

    public void setNodeAttributes(Map<String,String> updatedAttributes) throws ZoniException,
            IOException {
        out.writeInt(ZoniProtocol.OPCODE_SET_NODE_ATTRIBUTES);

        out.writeStringMap(updatedAttributes);

        out.flush();

        int status = in.readInt();
        String message = in.readString();

        if (status != ZoniProtocol.STATUS_OK) {
            close();
            throw new ZoniException("exception on setting peer attributes: "
                    + message);
        }

    }

    public void kill(boolean recursive) throws ZoniException, IOException {
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
            throw new ZoniException("exception on killing peer: " + message);
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
