package nl.vu.zorilla.cluster;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.UUID;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.NodeInfo;
import nl.vu.zorilla.Service;

import org.apache.log4j.Logger;

import smartsockets.direct.DirectSocket;

/**
 * Class taking care of maintaining a list of "close-by" nodes
 */
public abstract class ClusterService implements Service {

    public static final byte OPCODE_NEIGHBOUR_INFO_REQUEST = 1;

    private static final Logger logger = Logger.getLogger(ClusterService.class);

    public static ClusterService getClusterService(Node node)
            throws IOException {
        return new NearestNodeClustering(node);
    }

    protected final Node node;

    public ClusterService(Node node) throws IOException {
        this.node = node;
    }

    public abstract void start();
    
    public abstract Neighbour[] getSortedNeighbours();

    public abstract NodeInfo[] getNeighbourInfos();

    protected abstract Neighbour getNeighbour(UUID id);

    public abstract NodeInfo getRandomNeighbour();
    
    public abstract double distanceToClosestNeighbour();

    public void handleConnection(DirectSocket socket) {
        try {
            byte opcode = (byte) socket.getInputStream().read();

            if (opcode != OPCODE_NEIGHBOUR_INFO_REQUEST) {
                throw new IOException("received illegal opcode " + opcode);
            }

            ObjectOutputStream out = new ObjectOutputStream(socket
                    .getOutputStream());
            out.writeObject(node.getInfo());
            out.flush();
            out.close();
            socket.close();
        } catch (IOException e) {
            logger.error("error on handling neighbour request", e);
            try {
                socket.close();
            } catch (Exception e2) {
                // IGNORE
            }
        }
    }


}
