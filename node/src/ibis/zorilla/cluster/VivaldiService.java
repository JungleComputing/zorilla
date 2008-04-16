package ibis.zorilla.cluster;

import ibis.util.ThreadPool;
import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;
import ibis.zorilla.Service;
import ibis.zorilla.net.Network;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;


import org.apache.log4j.Logger;

import ibis.smartsockets.direct.DirectSocket;

public class VivaldiService implements Service, Runnable {

    // how long do we wait for a connection
    public static final int CONNECTION_TIMEOUT = 10 * 1000;

    // how long do we wait between two "pings"
    public static final int PING_INTERVAL = 10 * 1000;

    public static final int TRIES = 4;

    private final Logger logger = Logger.getLogger(VivaldiService.class);

    private final Node node;

    private Coordinates coordinates;

    public VivaldiService(Node node) throws IOException {
        this.node = node;
        this.coordinates = new Coordinates();
    }

    public double ping(NodeInfo peer) throws IOException {
        return ping(peer, false);
    }

    private double ping(NodeInfo peer, boolean updateCoordinates)
            throws IOException {
        double result = Double.MAX_VALUE;

        DirectSocket socket = node.network().connect(peer,
                Network.VIVALDI_SERVICE, CONNECTION_TIMEOUT);
        socket.setTcpNoDelay(true);

        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();

        // get coordinates from peer
        byte[] coordinateBytes = new byte[Coordinates.SIZE];
        int remaining = Coordinates.SIZE;
        int offset = 0;
        while (remaining > 0) {
            int read = in.read(coordinateBytes, offset, remaining);
            if (read == -1) {
                throw new IOException("could not read Coordinates");
            }
            offset += read;
            remaining -= read;
        }
        Coordinates remoteCoordinates = new Coordinates(coordinateBytes);

        for (int i = 0; i < TRIES; i++) {
            long start = System.nanoTime();
            out.write(i);
            out.flush();
            int reply = in.read();
            long end = System.nanoTime();
            if (reply != i) {
                throw new IOException("ping failed, wrong reply: " + reply);
            }

            long time = end - start;
            double rtt = (double) time / 1000000.0;

            if (rtt < result) {
                result = rtt;
            }
        }
        socket.close();

        if (updateCoordinates) {
            updateCoordinates(remoteCoordinates, result);
        }

        logger.debug("distance to " + peer + " is " + result + " ms");

        return result;
    }

    public void handleConnection(DirectSocket socket) {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        try {
            socket.setTcpNoDelay(true);

            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            // send coordinates
            out.write(getCoordinates().toBytes());
            out.flush();

            for (int i = 0; i < TRIES; i++) {
                int read = in.read();
                out.write(read);
                out.flush();
            }
            socket.close();
        } catch (IOException e) {
            logger.error("error on handling ping", e);
        }
        Thread.currentThread().setPriority(Thread.NORM_PRIORITY);
    }
    

    public void start() {
        ThreadPool.createNew(this, "vivaldi");
        logger.info("Started Vivaldi service");
    }

    private synchronized void updateCoordinates(Coordinates remoteCoordinates,
            double rtt) {
        coordinates = coordinates.update(remoteCoordinates, rtt);
    }

    public synchronized Coordinates getCoordinates() {
        return coordinates;
    }

    public void run() {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        while (true) {
            NodeInfo neighbour = node.clusterService().getRandomNeighbour();
            if (neighbour != null) {
                try {
                    ping(neighbour, true);
                } catch (Exception e) {
                    logger.debug("error on pinging neighbour " + neighbour, e);

                }
            }

            NodeInfo randomNode = node.gossipService().getRandomNode();
            if (randomNode != null) {
                try {
                    ping(randomNode, true);
                } catch (Exception e) {
                    logger.debug("error on pinging random node " + randomNode,
                            e);

                }
            }

            try {
                Thread.sleep(PING_INTERVAL);
            } catch (InterruptedException e) {
                // IGNORE
            }
        }

    }

    public Map<String, String> getStats() {
        HashMap<String,String> result = new HashMap<String,String>();
        
        result.put("Current.coordinate", getCoordinates().toString());

        return result;
    }
}