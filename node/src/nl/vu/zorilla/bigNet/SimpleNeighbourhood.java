package nl.vu.zorilla.bigNet;

import ibis.util.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;

import nl.vu.zorilla.Node;

import org.apache.log4j.Logger;

import smartsockets.direct.DirectSocket;

/**
 * Keeps a list of neighbours sorted by distance to this node. Has a tendancy to
 * create a fragmented network.
 */
class SimpleNeighbourhood extends NeighbourService implements Runnable {

    private static final Logger logger = Logger
            .getLogger(SimpleNeighbourhood.class);

    public static final int MAX_NEIGHBOURS = 50;

    // how many candidates do we have
    public static final int CANDIDATES = 5;

    public static final int TIMEOUT = 60 * 1000;

    private HashMap<UUID, Neighbour> neighbours;

    public SimpleNeighbourhood(BigNet network) throws IOException {
        super(network);

        neighbours = new HashMap<UUID, Neighbour>();

        ThreadPool.createNew(this, "neighbourhood watch");
    }

    protected synchronized Neighbour getNeighbour(UUID id) {
        return neighbours.get(id);
    }

    public synchronized NodeInfo[] getNodes() {
        ArrayList<NodeInfo> result = new ArrayList<NodeInfo>();

        for (Neighbour neighbour : neighbours.values()) {
            result.add(neighbour.getInfo());
        }

        return result.toArray(new NodeInfo[0]);

    }

    public synchronized int nrOfNeighbours() {
        return neighbours.size();
    }

    @Override
    public double distanceToClosestNode() {
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

    @Override
    protected synchronized void newNode(NodeInfo info) {
        if (neighbours.containsKey(info.getID())) {
            logger.debug("new node " + info + " already a neighbour");
            return;
        }

        if (info.sameNodeAs(network.getNodeInfo())) {
            logger.debug("tried to add ourselves as neighbour");
            return;
        }

        Neighbour newNeighbour = new Neighbour(network, info);
        logger.debug("new neighbour from new node: " + info);
        neighbours.put(newNeighbour.getID(), newNeighbour);

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

            String message = network.getNodeName()
                    + " sorted list of neighbours: ";

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

        while ((sorted.size() - nrOfCandidates) > MAX_NEIGHBOURS) {
            Neighbour removed = sorted.remove(sorted.size() - 1);
            neighbours.remove(removed.getID());
            logger.debug("removing " + removed + " from neighbour list");
            removed.end();
        }

        int newCandidates = (MAX_NEIGHBOURS + CANDIDATES) - neighbours.size();

        logger.debug("adding " + newCandidates
                + " new nodes to neighbours list");

        if (newCandidates < 0) {
            return;
        }

        NodeInfo[] newCandidateInfos = network.getRandomNodes(newCandidates);

        for (NodeInfo newInfo : newCandidateInfos) {
            if (newInfo != null && !newInfo.sameNodeAs(network.getNodeInfo())
                    && !neighbours.containsKey(newInfo.getID())) {
                Neighbour newNeighbour = new Neighbour(network, newInfo);
                logger.debug("new candidate: " + newInfo);
                neighbours.put(newNeighbour.getID(), newNeighbour);
            }
        }

    }

    public void run() {
        while (true) {
            updateNeighbourList();

            try {
                Thread.sleep(Node.randomTimeout(TIMEOUT));
            } catch (InterruptedException e) {
                // IGNORE
            }

        }
    }


}
