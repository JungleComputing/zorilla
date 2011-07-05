package ibis.zorilla.net;

import ibis.zorilla.cluster.Coordinates;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class ConnectionSet {

    private static final class LatencyComparator implements
            Comparator<Connection> {

        public int compare(Connection one, Connection other) {
            if (one.distanceMs() < other.distanceMs()) {
                return -1;
            } else if (one.distanceMs() > other.distanceMs()) {
                return 1;
            } else {
                return 0;
            }
        }

    }

    private static Connection[] sortConnectionsByDistance(
            Collection<Connection> unsorted) {
        Connection[] array = unsorted.toArray(new Connection[0]);

        Arrays.sort(array, new LatencyComparator());

        return array;
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

    private final ArrayList<Connection> connections;

    ConnectionSet() {
        connections = new ArrayList<Connection>();
    }

    double distanceToClosestConnection() {
        double result = Double.POSITIVE_INFINITY;

        for (Connection connection : connections) {
            double distance = connection.distanceMs();

            if (distance < result) {
                result = distance;
            }
        }
        return result;
    }

    /**
     * Purges unreachable neighbours. Removes furthest neighbours if there are
     * to many neighbours. If possible adds a candidate new neighbour.
     */
    protected synchronized void updateConnectionList() {
        int nrOfCandidates = 0;

        Connection[] sorted = sortConnectionsByDistance(connections);

        if (logger.isDebugEnabled()) {

            String message = node + " sorted list of neighbours: ";

            for (Connection neighbour : sorted) {
                message += neighbour + " ";
            }

            logger.debug(message);

        }

        Iterator<Connection> iterator = sorted.iterator();
        while (iterator.hasNext()) {
            Connection neighbour = iterator.next();
            double distance = neighbour.distanceMs();
            if (distance == Double.POSITIVE_INFINITY) {
                iterator.remove();
                neighbours.remove(neighbour.getID());

            } else if (distance == Double.NEGATIVE_INFINITY) {
                nrOfCandidates++;
            }
        }

        while ((sorted.size() - nrOfCandidates) > maxConnections) {
            Connection removed = sorted.remove(sorted.size() - 1);
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
                Connection newConnection = new Connection(node, newInfo);
                logger.debug("new candidate: " + newInfo);
                neighbours.put(newConnection.getID(), newConnection);
                added++;
                if (newInfo.isHub()) {
                    node.getIPLServer().addHubs(newInfo.getAddress().machine());
                }
            }
        }

    }

}
