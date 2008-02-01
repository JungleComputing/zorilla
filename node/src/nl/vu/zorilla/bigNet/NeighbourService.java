package nl.vu.zorilla.bigNet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.util.UUID;


import org.apache.log4j.Logger;

import smartsockets.direct.DirectSocket;

/**
 * Class taking care of maintaining a list of "close-by" nodes
 */
abstract class NeighbourService implements Service {

    public static final int OPCODE_NEIGHBOUR_INFO_REQUEST = 1;

    public static final int OPCODE_NEIGHBOUR_INFO_REPLY = 2;

    private static final Logger logger = Logger
            .getLogger(NeighbourService.class);

    protected final BigNet network;

    public NeighbourService(BigNet network) throws IOException {
        this.network = network;
    }
    
    public void start() {
        //IGNORE
    }

    public void handleNeighbourInfoRequest(ObjectInputStream in,
            DatagramPacket packet) {
        try {
            NodeInfo requestor = (NodeInfo) in.readObject();
            UUID destination = (UUID) in.readObject();

            if (!destination.equals(network.getNodeID())) {
                logger
                        .error("got neighbour info request for different node: "
                                + destination
                                + " (we are "
                                + network.getNodeID() + ")");
            }

            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            byteOut.write(BigNet.NEIGHBOUR_SERVICE);
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeInt(OPCODE_NEIGHBOUR_INFO_REPLY);
            out.writeObject(network.getNodeInfo());
            out.close();

            byte[] data = byteOut.toByteArray();

            network.send(data, requestor);

        } catch (IOException e) {
            logger.error("could not handle neighbour info request", e);
        } catch (ClassNotFoundException e) {
            logger.error("could not handle neighbour info request", e);

        }
    }

    public void handleNeighbourInfoReply(ObjectInputStream in,
            DatagramPacket packet) {
        try {

            NodeInfo info = (NodeInfo) in.readObject();

            Neighbour neighbour = getNeighbour(info.getID());

            if (neighbour == null) {
                logger.error("info recieved for unknown neighbour");
                return;
            }
            neighbour.updateInfo(info);

        } catch (IOException e) {
            logger.error("could not handle neighbour info request", e);
        } catch (ClassNotFoundException e) {
            logger.error("could not handle neighbour info request", e);

        }

    }

    public abstract NodeInfo[] getNodes();

    protected abstract Neighbour getNeighbour(UUID id);

    protected abstract void newNode(NodeInfo info);

    public abstract double distanceToClosestNode();

    public void handleMessage(DatagramPacket packet) {
        try {
            InputStream bytes = new ByteArrayInputStream(packet.getData());
            bytes.skip(1);
            ObjectInputStream in = new ObjectInputStream(bytes);
            int opcode = in.readInt();

            switch (opcode) {
            case OPCODE_NEIGHBOUR_INFO_REQUEST:
                handleNeighbourInfoRequest(in, packet);
                break;
            case OPCODE_NEIGHBOUR_INFO_REPLY:
                handleNeighbourInfoReply(in, packet);
                break;
            default:
                logger.error("unknown opcode in received message: " + opcode);
            }
        } catch (IOException e) {
            logger.error("error on handling message in neighbour service", e);
        }
    }
    
    public void handleConnection(DirectSocket socket) {
        logger.error("connection to UDP based neighbour service");
    }

}