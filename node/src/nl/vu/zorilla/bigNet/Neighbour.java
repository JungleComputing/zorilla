package nl.vu.zorilla.bigNet;

import ibis.util.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.UUID;

import nl.vu.zorilla.Node;

import org.apache.log4j.Logger;

class Neighbour implements Runnable {

    /**
     * if true, the avarage of the ping history is used. by default the mimimm
     * is returned
     */
    public static final boolean AVERAGE_PING_TIMES = false;

    public static final int TIMEOUT = 60 * 1000;

    public static final long LIFETIME = 10 * 60 * 1000;

    public static final int PING_HISTORY_LENGTH = 5;

    public static final int PING_FAIL_COUNT = 5;

    public static final int PING_TIMEOUT = 5 * 1000;

    private static final Logger logger = Logger.getLogger(Neighbour.class);

    private final BigNet network;

    private long expirationTime;

    private long failures;

    private final LinkedList<Double> pingTimes;

    private NodeInfo info;

    private final UUID id;
    
    private boolean ended = false;

    Neighbour(BigNet network, NodeInfo node) {
        this.network = network;
        this.info = node;

        id = node.getID();

        failures = 0;

        this.pingTimes = new LinkedList<Double>();

        resetExpirationClock();

        ThreadPool.createNew(this, "neighbour bootstrap thread");
    }

    private synchronized void resetExpirationClock() {
        expirationTime = System.currentTimeMillis() + LIFETIME;
    }

    private synchronized boolean isExpired() {
        return System.currentTimeMillis() > expirationTime;
    }

    public synchronized NodeInfo getInfo() {
        return info;
    }

    UUID getID() {
        return id;
    }

    void sendInfoUpdateRequest() {
        logger.debug("sending info request to " + id);
        try {

            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            byteOut.write(BigNet.NEIGHBOUR_SERVICE);
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeInt(NeighbourService.OPCODE_NEIGHBOUR_INFO_REQUEST);
            out.writeObject(network.getNodeInfo());
            out.writeObject(id);
            out.close();

            byte[] data = byteOut.toByteArray();

            network.send(data, info);

        } catch (IOException e) {
            logger.error("could not send ping", e);
        }
    }

    synchronized void updateInfo(NodeInfo newInfo) {
        if (!info.sameNodeAs(newInfo)) {
            logger.error("got pong from the wrong node: " + newInfo + " vs "
                    + info);
        }

        info = newInfo;
        resetExpirationClock();

        logger.debug("updated info of " + info);
    }

    /**
     * as a side effect will also update our vivaldi coordinates
     */
    private void doLatencyMeasurement() {
        if (logger.isDebugEnabled()) {
            logger.debug("sending ping to " + getInfo());
        }

        double rtt;
        try {
            rtt = network.doPing(info);
        } catch (IOException e) {
            logger.error("ping to " + info + " failed", e);
            failures++;
            return;
        }

        synchronized (this) {

            pingTimes.add(rtt);
            failures = 0;

            if (logger.isDebugEnabled()) {
                logger.debug("ping to " + info + " took " + rtt
                        + " milliseconds");
            }

            while (pingTimes.size() > PING_HISTORY_LENGTH) {
                pingTimes.remove();
            }
        }
    }

    /**
     * returns the average of the last PING_HISTORY_LENGTH pings
     */
    synchronized double distanceMs() {
        if (ended) {
            return Double.POSITIVE_INFINITY;
        }
        
        if (failures > PING_FAIL_COUNT) {
            return Double.POSITIVE_INFINITY; // unreachable a.k.a _very_ far
        }

        if (isExpired()) {
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
            doLatencyMeasurement();
        }

        while (distanceMs() != Double.POSITIVE_INFINITY) {
            doLatencyMeasurement();

            sendInfoUpdateRequest();

            try {
                Thread.sleep(Node.randomTimeout(TIMEOUT));
            } catch (InterruptedException e) {
                // IGNORE
            }
        }

    }

    public synchronized void end() {
        ended = true;
    }

}