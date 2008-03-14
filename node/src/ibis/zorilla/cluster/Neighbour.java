package ibis.zorilla.cluster;

import ibis.util.ThreadPool;
import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;
import ibis.zorilla.net.Network;

import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.UUID;


import org.apache.log4j.Logger;

import ibis.smartsockets.direct.DirectSocket;

public class Neighbour implements Runnable {

    /**
     * if true, the avarage of the ping history is used. by default the mimimm
     * is returned
     */
    public static final boolean AVERAGE_PING_TIMES = false;

    public static final int REQUEST_TIMEOUT = 10 * 1000;

    public static final int TIMEOUT = 60 * 1000;

    public static final int MAX_FAIL_COUNT = 5;

    public static final int PING_HISTORY_LENGTH = 5;

    private static final Logger logger = Logger.getLogger(Neighbour.class);

    private long failures;

    private final LinkedList<Double> pingTimes;

    private Node node;

    // neighbour info
    private NodeInfo info;

    // neighbour ID
    private final UUID id;

    private boolean ended = false;

    Neighbour(Node node, NodeInfo info) {
        this.node = node;
        this.info = info;

        id = info.getID();

        failures = 0;

        this.pingTimes = new LinkedList<Double>();

        ThreadPool.createNew(this, "neighbour bootstrap thread");
    }

    public synchronized NodeInfo getInfo() {
        return info;
    }

    UUID getID() {
        return id;
    }

    private void updateInfo() {
        try {
            NodeInfo oldInfo = getInfo();
            DirectSocket socket = node.network().connect(oldInfo,
                    Network.CLUSTER_SERVICE, REQUEST_TIMEOUT);

            OutputStream out = socket.getOutputStream();
            out.write(ClusterService.OPCODE_NEIGHBOUR_INFO_REQUEST);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket
                    .getInputStream());

            NodeInfo newInfo;
            try {
                newInfo = (NodeInfo) in.readObject();
            } catch (ClassNotFoundException e) {
                throw new Exception("received unknown class", e);
            }

            if (!oldInfo.sameNodeAs(newInfo)) {
                logger.error("got pong from the wrong node: " + newInfo
                        + " vs " + oldInfo);
            }

            // uses a new connection to the peer node
            double rtt = node.vivaldiService().ping(newInfo);

            synchronized (this) {
                info = newInfo;

                pingTimes.add(rtt);

                if (logger.isDebugEnabled()) {
                    logger.debug("ping to " + info + " took " + rtt
                            + " milliseconds");
                }

                while (pingTimes.size() > PING_HISTORY_LENGTH) {
                    pingTimes.remove();
                }
                failures = 0;
            }
        } catch (Exception e) {
            logger.warn("failed to update neighbour " + getInfo());
            logger.debug("failed to update neighbour " + getInfo(), e);

            synchronized (this) {
                failures++;
            }
        }
    }

    /**
     * returns the average of the last PING_HISTORY_LENGTH pings
     */
    public synchronized double distanceMs() {
        if (ended) {
            return Double.POSITIVE_INFINITY;
        }

        if (failures > MAX_FAIL_COUNT) {
            return Double.POSITIVE_INFINITY; // unreachable a.k.a _very_ far
        }

        if (pingTimes.size() < PING_HISTORY_LENGTH) {
            return Double.NEGATIVE_INFINITY; // unknown (yet)
        }

        if (AVERAGE_PING_TIMES) {
            // take average of availale measurements
            double totalTime = 0;

            for (double time : pingTimes) {
                totalTime += time;
            }

            return totalTime / pingTimes.size();
        } else {
            // take minimum of available measurements
            double minimum = Double.POSITIVE_INFINITY;

            for (double time : pingTimes) {
                if (time < minimum) {
                    minimum = time;
                }
            }

            return minimum;
        }

    }

    public String toString() {
        return info + "(" + distanceMs() + " ms)";
    }

    public void run() {
        // do a number of distance measurements to get a first distance estimate
        while (distanceMs() == Double.NEGATIVE_INFINITY) {
            updateInfo();
        }

        while (distanceMs() != Double.POSITIVE_INFINITY) {
            updateInfo();

            try {
                Thread.sleep(Node.randomTimeout(TIMEOUT));
            } catch (InterruptedException e) {
                // IGNORE
            }
        }

    }

    synchronized void end() {
        ended = true;
    }

}