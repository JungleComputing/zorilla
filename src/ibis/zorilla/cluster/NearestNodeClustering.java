package ibis.zorilla.cluster;

import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.net.NodeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

/**
 * Keeps a list of neighbors sorted by distance to this node. Has a tendency to
 * create a fragmented network.
 */
public class NearestNodeClustering extends ClusterService {

    private static final Logger logger = Logger
            .getLogger(NearestNodeClustering.class);

    private final int maxNeighbours;

    // how many candidates do we have
    public static final int CANDIDATES = 3;

    public static final int TIMEOUT = 10 * 1000;

    private HashMap<UUID, Neighbour> neighbours;

    public NearestNodeClustering(Node node) throws IOException {
        super(node);

        maxNeighbours = node.config().getIntProperty(
                Config.MAX_CLUSTER_SIZE);

        neighbours = new HashMap<UUID, Neighbour>();
    }

    @Override
    public void start() {
        ThreadPool.createNew(this, "neighbourhood watch");
        logger.info("Started Cluster service (nearest neighbour algorithm)");
    }

    protected synchronized Neighbour getNeighbour(UUID id) {
        return neighbours.get(id);
    }

    public synchronized Neighbour[] getSortedNeighbours() {
        return sortNeighboursByDistance(neighbours.values()).toArray(
                new Neighbour[0]);
    }

    public synchronized Neighbour[] getNeighbours() {
        return neighbours.values().toArray(new Neighbour[0]);
    }

    public synchronized NodeInfo[] getNeighbourInfos() {
        ArrayList<NodeInfo> result = new ArrayList<NodeInfo>();

        for (Neighbour neighbour : neighbours.values()) {
            result.add(neighbour.getInfo());
        }

        return result.toArray(new NodeInfo[0]);

    }

    public synchronized NodeInfo getRandomNeighbour() {
        Neighbour[] entries = neighbours.values().toArray(new Neighbour[0]);

        if (entries.length == 0) {
            return null;
        }

        return entries[Node.randomInt(entries.length)].getInfo();
    }

    public synchronized int nrOfNeighbours() {
        return neighbours.size();
    }

 

    public void run() {
        while (true) {
            for (Neighbour neighbour : getNeighbours()) {
                neighbour.ping();
            }

            updateNeighbourList();

            try {
                Thread.sleep(Node.randomTimeout(TIMEOUT));
            } catch (InterruptedException e) {
                // IGNORE
            }

        }
    }

    public Map<String, String> getStats() {
        HashMap<String, String> result = new HashMap<String, String>();

        return result;
    }

}
