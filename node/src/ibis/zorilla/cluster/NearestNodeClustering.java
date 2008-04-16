package ibis.zorilla.cluster;

import ibis.util.ThreadPool;
import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;

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
 * Keeps a list of neighbours sorted by distance to this node. Has a tendancy to
 * create a fragmented network.
 */
public class NearestNodeClustering extends ClusterService implements Runnable {

    private static final Logger logger = Logger
            .getLogger(NearestNodeClustering.class);

    private final int maxNeighbours;

    // how many candidates do we have
    public static final int CANDIDATES = 3;

    public static final int TIMEOUT = 60 * 1000;

    private HashMap<UUID, Neighbour> neighbours;

    public NearestNodeClustering(Node node) throws IOException {
        super(node);

        maxNeighbours = node.config().getIntProperty(Config.MAX_CLUSTER_SIZE);

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

    @Override
    public double distanceToClosestNeighbour() {
        double result = Double.POSITIVE_INFINITY;

        for (Neighbour neighbour : neighbours.values()) {
            double distance = neighbour.distanceMs();

            if (distance < result) {
                result = distance;
            }
        }
        return result;
    }

    private static final class LatencyComparator implements
            Comparator<Neighbour> {

        public int compare(Neighbour one, Neighbour other) {
            if (one.distanceMs() < other.distanceMs()) {
                return -1;
            } else if (one.distanceMs() > other.distanceMs()) {
                return 1;
            } else {
                return 0;
            }
        }

    }

    protected static ArrayList<Neighbour> sortNeighboursByDistance(
            Collection<Neighbour> unsorted) {
        Neighbour[] array = unsorted.toArray(new Neighbour[0]);

        Arrays.sort(array, new LatencyComparator());

        ArrayList<Neighbour> result = new ArrayList<Neighbour>();

        for (Neighbour neighbour : array) {
            result.add(neighbour);
        }

        return result;
    }

    private static final class NodeLatencyComparator implements
            Comparator<NodeInfo> {

        private Coordinates center;

        NodeLatencyComparator(Coordinates center) {
            this.center = center;
        }

        public int compare(NodeInfo one, NodeInfo other) {
            double result = one.getCoordinates().distance(center)
                    - other.getCoordinates().distance(center);
            if (result < 0) {
                return -1;
            } else if (result > 0) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    protected static void sortNodesByDistance(NodeInfo[] infos,
            Coordinates center) {
        Arrays.sort(infos, new NodeLatencyComparator(center));
    }

    /**
     * Perges unreachable neighbours. Removes furthest neighbours if there are
     * to many neighbours. If possible adds a candidate new neighbour.
     */
    protected synchronized void updateNeighbourList() {
        int nrOfCandidates = 0;

        ArrayList<Neighbour> sorted = sortNeighboursByDistance(neighbours
                .values());

        if (logger.isDebugEnabled()) {

            String message = node + " sorted list of neighbours: ";

            for (Neighbour neighbour : sorted) {
                message += neighbour + " ";
            }

            logger.debug(message);

        }

        Iterator<Neighbour> iterator = sorted.iterator();
        while (iterator.hasNext()) {
            Neighbour neighbour = iterator.next();
            double distance = neighbour.distanceMs();
            if (distance == Double.POSITIVE_INFINITY) {
                iterator.remove();
                neighbours.remove(neighbour.getID());

            } else if (distance == Double.NEGATIVE_INFINITY) {
                nrOfCandidates++;
            }
        }

        while ((sorted.size() - nrOfCandidates) > maxNeighbours) {
            Neighbour removed = sorted.remove(sorted.size() - 1);
            neighbours.remove(removed.getID());
            logger.debug("removing " + removed + " from neighbour list");
        }

        int newCandidates = CANDIDATES - nrOfCandidates;

        logger.debug("adding " + newCandidates
                + " new nodes to neighbours list");

        if (newCandidates < 0) {
            return;
        }

        NodeInfo[] newCandidateInfos = node.gossipService().getNodesList();

        sortNodesByDistance(newCandidateInfos, node.vivaldiService()
                .getCoordinates());

        int added = 0;
        for (NodeInfo newInfo : newCandidateInfos) {
            if (added >= newCandidates) {
                return;
            }
            if (newInfo != null && !newInfo.sameNodeAs(node.getInfo())
                    && !neighbours.containsKey(newInfo.getID())) {
                Neighbour newNeighbour = new Neighbour(node, newInfo);
                logger.debug("new candidate: " + newInfo);
                neighbours.put(newNeighbour.getID(), newNeighbour);
                added++;
            }
        }

    }

    public void run() {
        while (true) {
            for(Neighbour neighbour: getNeighbours()) {
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
