package nl.vu.zorilla.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;

import java.net.Socket;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.job.Job;
import nl.vu.zorilla.zoni.ZoniInputStream;
import nl.vu.zorilla.zoni.ZoniOutputStream;

import nl.vu.zorilla.zoni.ZoniProtocol;

import org.apache.log4j.Logger;
import org.gridlab.gat.URI;

final class ServerConnection implements Runnable {

    Logger logger = Logger.getLogger(ServerConnection.class);

    private final Node node;

    private final Socket socket;

    private final ZoniInputStream in;

    private final ZoniOutputStream out;

    public ServerConnection(Socket socket, Node node) throws IOException {
        this.socket = socket;
        this.node = node;

        logger.debug("new connection handler from: " + socket.getInetAddress()
                + ":" + socket.getPort());

        in = new ZoniInputStream(new BufferedInputStream(socket
                .getInputStream()));
        out = new ZoniOutputStream(new BufferedOutputStream(socket
                .getOutputStream()));

        new Thread(this).start();
    }

    // out.writeInt(ZoniProtocol.OPCODE_SUBMIT_JOB);
    // out.writeString(executable);
    //
    // // arguments;
    // out.writeStrings(arguments);
    //
    // logger.debug("writing environment:");
    // out.writeStringMap(environment);
    // logger.debug("writing attributes");
    // out.writeStringMap(attributes);
    //
    // logger.debug("writing pre stage:");
    // out.writeStringMap(preStage);
    // logger.debug("writing post stage:");
    // out.writeStringMap(postStage);
    //
    // out.flush();

    private void submitJob() throws IOException, Exception {
        URI executable;
        try {
            executable = new URI(in.readString());
        } catch (URISyntaxException e) {
            throw new Exception("could not create URI from string", e);
        }
        String[] arguments = in.readStrings();

        Map<String, String> environment = in.readStringMap();
        Map<String, String> attributes = in.readStringMap();
        Map<String, String> preStage = in.readStringMap();
        Map<String, String> postStage = in.readStringMap();
        String stdin = in.readString();
        String stdout = in.readString();
        String stderr = in.readString();

        Job job = node.jobService().submitJob(executable, arguments, environment,
                attributes, preStage, postStage, stdout, stdin, stderr);

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeString("OK");
        out.writeString(job.getID().toString());
        out.flush();

    }

    private void getJobInfo() throws Exception {

        String jobIDString = in.readString();

        logger.debug("receive info request for " + jobIDString);

        UUID jobID = UUID.fromString(jobIDString);
        Job job = node.jobService().getJob(jobID);

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeString("OK");
        out.writeString(job.getID().toString());
        out.writeString(job.getExecutable().toString());
        out.writeStringMap(job.getAttributes());
        out.writeStringMap(job.getStats());
        out.writeInt(job.getPhase());
        out.flush();

        logger.debug("done handling info request");

    }

    private void setJobAttributes() throws Exception {

        String jobIDString = in.readString();
        Map<String, String> updatedAtrributes = in.readStringMap();

        logger.debug("receive attribute update request for " + jobIDString);

        UUID jobID = UUID.fromString(jobIDString);
        Job job = node.jobService().getJob(jobID);

        job.updateAttributes(updatedAtrributes);

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeString("OK");
        out.flush();

    }

    private void cancelJob() throws Exception {

        String jobIDString = in.readString();

        logger.debug("receive cancel job request for " + jobIDString);

        UUID jobID = UUID.fromString(jobIDString);
        Job job = node.jobService().getJob(jobID);

        job.cancel();

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeString("OK");
        out.flush();

    }

    private void getJobList() throws IOException, Exception {
        Job[] jobs = node.jobService().getJobs();

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeString("OK");
        out.writeInt(jobs.length);
        for (int i = 0; i < jobs.length; i++) {
            out.writeString(jobs[i].getID().toString());
        }
        out.flush();
    }

    private void killNode() throws IOException, Exception {
        boolean recursive = (in.readInt() == 1);

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeString("OK");
        out.flush();
        close();

        
        if (recursive) {
        node.floodService().killNetwork();
        node.stop(FloodService.NETWORK_KILL_TIMEOUT);
        } else {
            node.stop(0);
        }
    }

    private void getNodeInfo() throws IOException, Exception {
        Map<String, String> info = node.getStats();

        out.writeInt(ZoniProtocol.STATUS_OK);
        out.writeString("OK");
        out.writeStringMap(info);
        out.flush();
    }

    private void setNodeAttributes() throws IOException, Exception {
        throw new Exception("cannot set node attributes");
    }

    public void run() {
        logger.debug("received new callback connection");

        try {
            int clientProtoVersion = in.readInt();
            int source = in.readInt();
            String id = in.readString();
            int authentication = in.readInt();

            logger.debug("new connection, protocol version = "
                    + clientProtoVersion + ", source = " + source + ", id = "
                    + id + " authentication = " + authentication);

            if (clientProtoVersion != 1) {
                throw new IOException("protocol version not supported");
            }

            if (authentication != ZoniProtocol.AUTHENTICATION_NONE) {
                out.writeInt(ZoniProtocol.STATUS_DENIED);
                out.writeString("only \"none\" autentication supported");
            }

            logger.debug("send back connection init ack");

            out.writeInt(ZoniProtocol.STATUS_OK);
            out.writeString("OK");
            out.writeString(node.getID().toString());
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
                    submitJob();
                    break;
                case ZoniProtocol.OPCODE_GET_JOB_INFO:
                    getJobInfo();
                    break;
                case ZoniProtocol.OPCODE_SET_JOB_ATTRIBUTES:
                    setJobAttributes();
                    break;
                case ZoniProtocol.OPCODE_CANCEL_JOB:
                    cancelJob();
                    break;
                case ZoniProtocol.OPCODE_GET_JOB_LIST:
                    getJobList();
                    break;
                case ZoniProtocol.OPCODE_GET_NODE_INFO:
                    getNodeInfo();
                    break;
                case ZoniProtocol.OPCODE_SET_NODE_ATTRIBUTES:
                    setNodeAttributes();
                    break;
                case ZoniProtocol.OPCODE_KILL_NODE:
                    killNode();
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
                out.writeString("error on handling request: " + e);
                out.flush();
            } catch (Exception e2) {
                // IGNORE
            }
            logger.warn("error on handling request", e);
        }
        close();
        logger.debug("user connection handler " + this + " exits");
    }

    void close() {
        try {
            in.close();
            out.close();
            socket.close();
        } catch (IOException e) {
            logger.warn("error on closing socket", e);
        }
    }
}
