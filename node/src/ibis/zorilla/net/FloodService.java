package ibis.zorilla.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;

import ibis.smartsockets.direct.DirectSocket;
import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;
import ibis.zorilla.Service;
import ibis.zorilla.cluster.Coordinates;
import ibis.zorilla.job.Advert;


public final class FloodService implements Service {

    private static final Logger logger = Logger.getLogger(FloodService.class);

    public static final long NETWORK_KILL_TIMEOUT = 10 * 1000;

    public static final int REQUEST_TIMEOUT = 5 * 1000;

    public static final int KILL_NETWORK_RADIUS = 100;

    public static final int OPCODE_NETWORK_KILL = 1;

    public static final int OPCODE_JOB_ADVERT_HOPS = 2;

    public static final int OPCODE_JOB_ADVERT_LATENCY = 3;

    private final Node node;

    public FloodService(Node node) {
        this.node = node;
    }

    public void start() {
        logger.info("Started Flood service");
    }

    private void killNetwork(int radius) {
        if (radius <= 0) {
            logger.debug("not sending out network kill, radius reached 0");
            return;
        }

        NodeInfo[] neighbours = node.clusterService().getNeighbourInfos();

        for (NodeInfo neighbour : neighbours) {
            try {
                DirectSocket socket = node.network().connect(
                        neighbour.getAddress(), Network.FLOOD_SERVICE,
                        REQUEST_TIMEOUT);
                socket.getOutputStream().write(OPCODE_NETWORK_KILL);
                DataOutputStream out = new DataOutputStream(socket
                        .getOutputStream());
                out.writeInt(radius);
                out.flush();
                // ack
                socket.getInputStream().read();
                socket.close();

            } catch (IOException e) {
                logger.error("could not send out network kill to " + neighbour,
                        e);
            }
        }
    }

    private void handleNetworkKill(DirectSocket socket) {
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            int radius = in.readInt();
            socket.getOutputStream().write(42);
            socket.close();

            killNetwork(radius - 1);

            node.stop(NETWORK_KILL_TIMEOUT);

        } catch (Exception e) {
            logger.error("could not handle/forward job advert", e);
        }
    }

    public void advertise(Advert advert) {
        logger.debug("advertizing : " + advert);
        
        String metric = advert.getMetric();
        
        if (metric == null) {
            metric = node.config().getProperty(Config.DEFAULT_FLOOD_METRIC);
        }

        if (metric.equalsIgnoreCase("hops")) {
            // default radius is incremental, start at "1" so we only reach
            // the neighbours
            advertiseHops(advert, advert.getCount() + 1);
        } else if (metric.equalsIgnoreCase("latency")) {

            double initialRadius = node.clusterService()
                    .distanceToClosestNeighbour();
            if (initialRadius == Double.POSITIVE_INFINITY) {
                initialRadius = 1.0;
            }

            double radius = initialRadius * Math.pow(2, advert.getCount());

            advertiseLatency(advert, radius);
        } else if (metric.equalsIgnoreCase("neighbours")) {
            //only send to neighbours
            advertiseHops(advert, 1);
        } else {
            logger.error("unknown flood metric: " + metric);
        }
    }

    private void advertiseHops(Advert advert, int ttl) {
        if (ttl <= 0) {
            logger.debug("not sending out advert, ttl reached 0");
            return;
        }

        NodeInfo[] neighbours = node.clusterService().getNeighbourInfos();

        for (NodeInfo neighbour : neighbours) {
            logger.debug("sending out advert: " + advert + " to " + neighbour);
            try {
                DirectSocket socket = node.network().connect(
                        neighbour.getAddress(), Network.FLOOD_SERVICE,
                        REQUEST_TIMEOUT);
                socket.getOutputStream().write(OPCODE_JOB_ADVERT_HOPS);
                ObjectOutputStream out = new ObjectOutputStream(socket
                        .getOutputStream());
                out.writeObject(advert);
                out.writeInt(ttl);
                out.flush();
                // ack
                socket.getInputStream().read();
                socket.close();

            } catch (IOException e) {
                logger.error("could not send out advert to " + neighbour, e);
            }
        }
    }

    private void handleJobAdvertHops(DirectSocket socket) {
        try {
            ObjectInputStream in = new ObjectInputStream(socket
                    .getInputStream());
            Advert advert = (Advert) in.readObject();
            int ttl = in.readInt();
            socket.getOutputStream().write(42);
            socket.getOutputStream().flush();
            socket.close();

            advertiseHops(advert, ttl - 1);

            node.jobService().handleJobAdvert(advert);

        } catch (Exception e) {
            logger.error("could not handle/forward job advert", e);
        }
    }

    private void advertiseLatency(Advert advert, double distance) {
        Coordinates here = node.vivaldiService().getCoordinates();

        HashSet<NodeInfo> nodes = new HashSet<NodeInfo>();
        nodes.addAll(Arrays.asList(node.clusterService().getNeighbourInfos()));
        nodes.addAll(Arrays.asList(node.gossipService().getNodesList()));

        for (NodeInfo info : nodes) {
            double distanceToNode = info.getCoordinates().distance(here);

            // endless-loop-preventer.
            if (distanceToNode <= 0.0) {
                distanceToNode = 1.0;
            }

            if (distanceToNode < distance && !info.sameNodeAs(node.getInfo())) {
                logger.debug("sending advert to " + info + ": "
                        + distanceToNode + " < " + distance);
                try {
                    DirectSocket socket = node.network().connect(
                            info.getAddress(), Network.FLOOD_SERVICE,
                            REQUEST_TIMEOUT);
                    socket.getOutputStream().write(OPCODE_JOB_ADVERT_LATENCY);
                    ObjectOutputStream out = new ObjectOutputStream(socket
                            .getOutputStream());
                    out.writeObject(advert);
                    out.writeDouble(distance - distanceToNode);
                    out.flush();
                    // ack
                    socket.getInputStream().read();
                    socket.close();

                } catch (IOException e) {
                    logger.error("could not send out advert to " + info, e);
                    //logger.debug("could not send out advert to " + info, e);
                }
            } else {
                logger.debug("not sending advert to " + info
                        + " too far away : " + distanceToNode + " > "
                        + distance);
            }

        }
    }

    private void handleJobAdvertLatency(DirectSocket socket) {
        try {
            ObjectInputStream in = new ObjectInputStream(socket
                    .getInputStream());
            Advert advert = (Advert) in.readObject();
            double radius = in.readDouble();
            socket.getOutputStream().write(42);
            socket.getOutputStream().flush();
            socket.close();

            advertiseLatency(advert, radius);

            node.jobService().handleJobAdvert(advert);

        } catch (Exception e) {
            logger.error("could not handle/forward job advert", e);
        }
    }

    public void killNetwork() throws Exception {
        killNetwork(KILL_NETWORK_RADIUS);
    }

    public void handleConnection(DirectSocket socket) {
        try {
            byte opcode = (byte) socket.getInputStream().read();

            switch (opcode) {
            case OPCODE_NETWORK_KILL:
                logger.debug("incoming network kill");
                handleNetworkKill(socket);
                break;
            case OPCODE_JOB_ADVERT_HOPS:
                logger.debug("incoming job advert (hop based)");
                handleJobAdvertHops(socket);
                break;
            case OPCODE_JOB_ADVERT_LATENCY:
                logger.debug("incoming job advert (latency based)");
                handleJobAdvertLatency(socket);
                break;
            default:
                logger.error("unknown opcode in received message: " + opcode);
            }
        } catch (IOException e) {
            logger.error("exception on handling message", e);
        }
    }

    public synchronized Map<String, String> getStats() {
        Map<String, String> result = new HashMap<String, String>();

        return result;
    }

}
