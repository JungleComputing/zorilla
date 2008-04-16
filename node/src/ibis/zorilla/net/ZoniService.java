package ibis.zorilla.net;

import ibis.zorilla.Node;
import ibis.zorilla.Service;
import ibis.zorilla.job.Job;
import ibis.zorilla.zoni.ZoniFileInfo;
import ibis.zorilla.zoni.ZoniProtocol;
import ibis.zorilla.zoni.ZorillaJobDescription;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import ibis.smartsockets.direct.DirectSocket;

public class ZoniService implements Service {

    private final Logger logger = Logger.getLogger(ZoniService.class);

    private final Node node;

    public ZoniService(Node node) throws IOException {
        this.node = node;

    }

    public void start() {
        logger.info("Zoni Service starter");
    }

    public synchronized Map<String, String> getStats() {
        Map<String, String> result = new HashMap<String, String>();

        return result;
    }

    private void submitJob(ObjectInputStream in, ObjectOutputStream out)
            throws IOException, Exception {
        ZorillaJobDescription jobDescription = new ZorillaJobDescription(in,
                node.config().getTmpDir());

        ZoniCallback callback = null;

        if (in.readBoolean()) {
            callback = new ZoniCallback((InetSocketAddress[]) in.readObject());
        }

        Job job = node.jobService().submitJob(jobDescription, callback);
        if (callback != null) {
            callback.setJob(job);
        }

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");
        out.writeUTF(job.getID().toString());
        out.flush();

    }

    private void getJobInfo(ObjectInputStream in, ObjectOutputStream out)
            throws Exception {

        String jobIDString = in.readUTF();

        logger.debug("receive info request for " + jobIDString);

        UUID jobID = UUID.fromString(jobIDString);
        Job job = node.jobService().getJob(jobID);

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");
        out.writeUTF(job.getID().toString());
        out.writeUTF(job.getDescription().getExecutable());
        out.writeObject(job.getAttributes().getStringMap());
        out.writeObject(job.getStats());
        out.writeInt(job.getPhase());
        out.writeInt(job.getExitStatus());
        out.flush();

        logger.debug("done handling info request");

    }

    @SuppressWarnings("unchecked")
    private void setJobAttributes(ObjectInputStream in, ObjectOutputStream out)
            throws Exception {

        String jobIDString = in.readUTF();
        Map<String, String> updatedAtrributes = (Map<String, String>) in
                .readObject();

        logger.debug("receive attribute update request for " + jobIDString);

        UUID jobID = UUID.fromString(jobIDString);
        Job job = node.jobService().getJob(jobID);

        job.updateAttributes(updatedAtrributes);

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");
        out.flush();

    }

    private void cancelJob(ObjectInputStream in, ObjectOutputStream out)
            throws Exception {

        String jobIDString = in.readUTF();

        logger.debug("receive cancel job request for " + jobIDString);

        UUID jobID = UUID.fromString(jobIDString);
        Job job = node.jobService().getJob(jobID);

        job.cancel();

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");
        out.flush();

    }

    private void getJobList(ObjectInputStream in, ObjectOutputStream out)
            throws IOException, Exception {
        Job[] jobs = node.jobService().getJobs();

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");
        out.writeInt(jobs.length);
        for (int i = 0; i < jobs.length; i++) {
            out.writeUTF(jobs[i].getID().toString());
        }
        out.flush();
    }

    private void killNode(ObjectInputStream in, ObjectOutputStream out)
            throws IOException, Exception {
        boolean recursive = in.readBoolean();

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");
        out.flush();

        if (recursive) {
            node.floodService().killNetwork();
            node.stop(FloodService.NETWORK_KILL_TIMEOUT);
        } else {
            node.stop(0);
        }
    }

    private void getNodeInfo(ObjectInputStream in, ObjectOutputStream out)
            throws IOException, Exception {
        Map<String, String> info = node.getStats();

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");
        out.writeObject(info);
        out.flush();
    }

    private void setNodeAttributes(ObjectInputStream in, ObjectOutputStream out)
            throws IOException, Exception {
        throw new Exception("cannot set node attributes");
    }

    private void getFileInfo(ObjectInputStream in, ObjectOutputStream out)
            throws Exception {
        String jobIDString = in.readUTF();
        UUID jobID = UUID.fromString(jobIDString);
        Job job = node.jobService().getJob(jobID);
        String sandboxPath = in.readUTF();

        ZoniFileInfo info = job.getFileInfo(sandboxPath);

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");

        out.writeObject(info);

        out.flush();
    }

    private void putStdin(ObjectInputStream in, ObjectOutputStream out)
            throws Exception {
        String jobIDString = in.readUTF();
        UUID jobID = UUID.fromString(jobIDString);
        Job job = node.jobService().getJob(jobID);

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");
        out.flush();

        job.writeStdin(in);
        out.close();
    }

    private void getStdout(ObjectInputStream in, ObjectOutputStream out)
            throws Exception {
        String jobIDString = in.readUTF();
        UUID jobID = UUID.fromString(jobIDString);
        Job job = node.jobService().getJob(jobID);

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");
        out.flush();

        job.readStdout(out);
        out.close();
    }

    private void getStderr(ObjectInputStream in, ObjectOutputStream out)
            throws Exception {
        String jobIDString = in.readUTF();
        UUID jobID = UUID.fromString(jobIDString);
        Job job = node.jobService().getJob(jobID);

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeUTF("OK");
        out.flush();

        job.readStderr(out);
        out.close();
    }

    private void getFile(ObjectInputStream in, ObjectOutputStream out)
            throws Exception {
        String jobIDString = in.readUTF();
        String sandboxPath = in.readUTF();

        UUID jobID = UUID.fromString(jobIDString);
        Job job = node.jobService().getJob(jobID);

        out.writeInt(ZoniProtocol.STATUS_OK);

        out.writeUTF("OK");
        out.flush();

        job.readOutputFile(sandboxPath, out);

        out.flush();
    }

    public void handleConnection(DirectSocket socket) {
        logger.debug("got new zoni connection");

        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        try {
            in = new ObjectInputStream(new BufferedInputStream(socket
                    .getInputStream()));
            out = new ObjectOutputStream(new BufferedOutputStream(socket
                    .getOutputStream()));

        } catch (IOException e) {
            logger.warn("error on handling request", e);
            try {
                socket.close();
            } catch (Exception e2) {
                // IGNORE
                return;
            }
        }

        try {

            int clientProtoVersion = in.readInt();
            String id = in.readUTF();
            int authentication = in.readInt();

            logger.debug("new connection, protocol version = "
                    + clientProtoVersion + ", id = " + id
                    + " authentication = " + authentication);

            if (clientProtoVersion != ZoniProtocol.VERSION) {
                throw new IOException("protocol version not supported");
            }

            if (authentication != ZoniProtocol.AUTHENTICATION_NONE) {
                out.writeInt(ZoniProtocol.STATUS_DENIED);
                out.writeUTF("only \"none\" autentication supported");
            }

            logger.debug("send back connection init ack");

            out.writeInt(ZoniProtocol.STATUS_OK);
            out.writeUTF("OK");
            out.writeUTF(node.getID().toString());
            out.flush();

            logger.debug("waiting for request");

            while (!socket.isClosed()) {
                logger.debug("waiting for request message");

                int opcode = in.readInt();

                logger.debug("received opcode: " + opcode);

                switch (opcode) {
                case ZoniProtocol.OPCODE_CLOSE_CONNECTION:
                    // close connection received
                    socket.close();
                    break;
                case ZoniProtocol.OPCODE_SUBMIT_JOB:
                    submitJob(in, out);
                    break;
                case ZoniProtocol.OPCODE_GET_JOB_INFO:
                    getJobInfo(in, out);
                    break;
                case ZoniProtocol.OPCODE_SET_JOB_ATTRIBUTES:
                    setJobAttributes(in, out);
                    break;
                case ZoniProtocol.OPCODE_CANCEL_JOB:
                    cancelJob(in, out);
                    break;
                case ZoniProtocol.OPCODE_GET_JOB_LIST:
                    getJobList(in, out);
                    break;
                case ZoniProtocol.OPCODE_GET_NODE_INFO:
                    getNodeInfo(in, out);
                    break;
                case ZoniProtocol.OPCODE_SET_NODE_ATTRIBUTES:
                    setNodeAttributes(in, out);
                    break;
                case ZoniProtocol.OPCODE_KILL_NODE:
                    killNode(in, out);
                    break;
                case ZoniProtocol.OPCODE_GET_FILE_INFO:
                    getFileInfo(in, out);
                    break;
                case ZoniProtocol.OPCODE_PUT_STDIN:
                    putStdin(in, out);
                    break;
                case ZoniProtocol.OPCODE_GET_STDOUT:
                    getStdout(in, out);
                    break;
                case ZoniProtocol.OPCODE_GET_STDERR:
                    getStderr(in, out);
                    break;
                case ZoniProtocol.OPCODE_GET_FILE:
                    getFile(in, out);
                    break;
                default:
                    throw new IOException("unknown/unsupported opcode: "
                            + opcode);
                }
            }

        } catch (EOFException e) {
            logger.debug("socket closed on receiving/handling request", e);
        } catch (Exception e) {
            try {
                out.writeInt(ZoniProtocol.STATUS_ERROR);
                out.writeUTF(e.getMessage());
                out.flush();
            } catch (Exception e2) {
                // IGNORE
            }
            logger.warn("error on handling request", e);
        } finally {
            try {
                in.close();
            } catch (IOException e2) {
                // IGNORE
            }
            try {
                out.close();
            } catch (IOException e2) {
                // IGNORE
            }
            try {
                socket.close();
            } catch (IOException e2) {
                // IGNORE
            }

        }

    }
}
