package nl.vu.zorilla.bigNet;

import java.io.IOException;
import java.util.UUID;

public class NullNeighbourService extends NeighbourService {

    public NullNeighbourService(BigNet network) throws IOException {
        super(network);
    }

    @Override
    public NodeInfo[] getNodes() {
        return new NodeInfo[0];
    }

    @Override
    protected Neighbour getNeighbour(UUID id) {
        return null;
    }

    @Override
    protected void newNode(NodeInfo info) {
        //IGNORE
    }

    @Override
    public double distanceToClosestNode() {
        return Double.POSITIVE_INFINITY;
    }

}
