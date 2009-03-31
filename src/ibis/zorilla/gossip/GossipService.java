package ibis.zorilla.gossip;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import ibis.smartsockets.virtual.VirtualSocket;
import ibis.zorilla.ZorillaTypedProperties;
import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;
import ibis.zorilla.Service;
import ibis.zorilla.net.Network;
import vu.platform934.analysis.Chart;

public class GossipService implements Service {

    private static final Logger logger = Logger.getLogger(GossipService.class);

    private final Node node;

    private Map<String, GossipAlgorithm> algorithms;

    private GossipAlgorithm mainAlgorithm;

    private final Random random;

    private final double messageLossFraction;

    private final long bootstrapTimeout;

    private final long disconnectTime;

    private final long reconnectTime;

    private final boolean wanDisconnect;

    public GossipService(Node node) throws Exception {
        this.node = node;
        ZorillaTypedProperties config = node.config();

        random = new Random();
        messageLossFraction = config
                .getIntProperty(ZorillaTypedProperties.MESSAGE_LOSS_PERCENTAGE) / 100.0;
        bootstrapTimeout = config
                .getIntProperty(ZorillaTypedProperties.BOOTSTRAP_TIMEOUT) * 1000;

        int disconnectSeconds = config
                .getIntProperty(ZorillaTypedProperties.DISCONNECT_TIME);
        if (disconnectSeconds > 0) {
            disconnectTime = System.currentTimeMillis()
                    + (disconnectSeconds * 1000);
        } else {
            disconnectTime = Long.MAX_VALUE;
        }

        int reconnectSeconds = config
                .getIntProperty(ZorillaTypedProperties.RECONNECT_TIME);
        if (reconnectSeconds > 0) {
            reconnectTime = System.currentTimeMillis()
                    + (reconnectSeconds * 1000);
        } else {
            reconnectTime = Long.MAX_VALUE;
        }

        if (reconnectTime < disconnectTime) {
            throw new Exception(
                    "cannot set reconnect time before disconnect time");
        }

        wanDisconnect = config
                .getBooleanProperty(ZorillaTypedProperties.WAN_DISCONNECT);

        int cacheSize = config
                .getIntProperty(ZorillaTypedProperties.GOSSIP_CACHE_SIZE);
        int sendSize = config
                .getIntProperty(ZorillaTypedProperties.GOSSIP_SEND_SIZE);
        int interval = config
                .getIntProperty(ZorillaTypedProperties.GOSSIP_INTERVAL) * 1000;

        algorithms = new HashMap<String, GossipAlgorithm>();

        File statsDir = new File(node.config().getLogDir(), node.getName()
                + "-gossip-stats");
        statsDir.mkdirs();

        GossipAlgorithm arrg = new ARRG("ARRG", true, true, cacheSize,
                sendSize, interval, this, statsDir, node.getID());
        algorithms.put(arrg.getName(), arrg);

        if (node.config().getBooleanProperty(
                ZorillaTypedProperties.ADDITIONAL_GOSSIP_ALGORITHMS)) {
            GossipAlgorithm noFallbackARRG = new ARRG("noFallbackARRG", false,
                    false, cacheSize, sendSize, interval, this, statsDir, node
                            .getID());
            algorithms.put(noFallbackARRG.getName(), noFallbackARRG);

            GossipAlgorithm retryARRG = new ARRG("RetryARRG", true, false,
                    cacheSize, sendSize, interval, this, statsDir, node.getID());
            algorithms.put(retryARRG.getName(), retryARRG);

            GossipAlgorithm cyclon = new Cyclon("Cyclon", false, false,
                    cacheSize, sendSize, interval, this, statsDir, node.getID());
            algorithms.put(cyclon.getName(), cyclon);

            GossipAlgorithm retryCyclon = new Cyclon("RetryCyclon", true,
                    false, cacheSize, sendSize, interval, this, statsDir, node
                            .getID());
            algorithms.put(retryCyclon.getName(), retryCyclon);

            GossipAlgorithm fallbackCyclon = new Cyclon("FallbackCyclon", true,
                    true, cacheSize, sendSize, interval, this, statsDir, node
                            .getID());
            algorithms.put(fallbackCyclon.getName(), fallbackCyclon);
        }

        mainAlgorithm = arrg;
    }

    public void start() {
        for (GossipAlgorithm algorithm : algorithms.values()) {
            algorithm.start();
        }
        // ThreadPool.createNew(this, "gossip service");
    }

    GossipMessage doTcpRequest(GossipMessage request, int timeout)
            throws IOException {

        long now = System.currentTimeMillis();

        if (now > disconnectTime && now < reconnectTime) {
            if (!wanDisconnect
                    || !request.getSender().getClusterName().equals(
                            request.getReceiver().getClusterName())) {
                throw new IOException("disconnected");
            } else {
                logger.info("allowing local request while disconnected");
            }
        }

        try {
            logger.debug("doing tcp request to " + request.getReceiver());
            VirtualSocket socket = node.network().connect(
                    request.getReceiver(), Network.GOSSIP_SERVICE, timeout);

            ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));
            out.writeObject(request);
            out.flush();

            logger.debug("send request, receiving reply");

            ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            GossipMessage reply = (GossipMessage) in.readObject();
            socket.close();

            logger.debug("reply received");

            if (!request.getReceiver().getID()
                    .equals(reply.getSender().getID())) {
                // logger.info("got gossip reply from wrong node, " +
                // request.getReceiver() + " instead of " + reply.getSender());
                throw new IOException("got reply from wrong node");
            }

            return reply;
        } catch (Throwable e) {
            throw new IOException("could not read reply object: " + e);
        }
    }

    public void handleConnection(VirtualSocket socket) {
        try {

            if (random.nextDouble() < messageLossFraction) {
                socket.close();
                logger.warn("Request message lost! (at random)");
                return;
            }

            ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            GossipMessage request = (GossipMessage) in.readObject();

            long now = System.currentTimeMillis();
            if (now > disconnectTime && now < reconnectTime) {
                if (!wanDisconnect
                        || !request.getSender().getClusterName().equals(
                                request.getReceiver().getClusterName())) {
                    socket.close();
                    logger.warn("Request not handled: we are disconnected");
                    return;
                } else {
                    logger.info("allowing local request while disconnected");
                }
            }

            GossipAlgorithm algorithm = algorithms.get(request
                    .getAlgorithmName());

            if (algorithm == null) {
                throw new IOException("could not find algorithm: "
                        + request.getAlgorithmName());
            }

            GossipMessage reply = algorithm.handleRequest(request);

            if (random.nextDouble() < messageLossFraction) {
                logger.warn("Reply message lost! (at random)");
            } else {
                ObjectOutputStream out = new ObjectOutputStream(
                        new BufferedOutputStream(socket.getOutputStream()));
                out.writeObject(reply);
                out.flush();
            }

            socket.close();
        } catch (Throwable e) {
            logger.error("error on handling request", e);
            try {
                socket.close();
            } catch (IOException e2) {
                // IGNORE
            }
        }
    }

    NodeInfo getBootstrapNode() {
        if (bootstrapTimeout > 0
                && (System.currentTimeMillis() > (node.getStartTime() + bootstrapTimeout))) {
            return null;
        }

        ArrayList<NodeInfo> nodes = new ArrayList<NodeInfo>();

        for (NodeInfo info : node.discoveryService().getNodesList()) {
            nodes.add(info);
        }
        for (NodeInfo info : node.udpDiscoveryService().getNodesList()) {
            nodes.add(info);
        }

        if (nodes.size() == 0) {
            return null;
        }

        return nodes.get(Node.randomInt(nodes.size()));
    }

    NodeInfo getNodeInfo() {
        return node.getInfo();
    }

    public NodeInfo[] getNodesList() {
        return mainAlgorithm.getNodes();
    }

    public NodeInfo[] getRandomNodes(int n) {
        return mainAlgorithm.getRandomNodes(n);
    }

    public NodeInfo getRandomNode() {
        return mainAlgorithm.getRandomNode();
    }

    public NodeInfo[] getFallbackNodesList() {
        return mainAlgorithm.getFallbackNodes();
    }

    public Map<String, String> getStats() {
        HashMap<String, String> result = new HashMap<String, String>();

        Stats stats = mainAlgorithm.getStats();

        result.put("Algorithm.Name", mainAlgorithm.getName());
        result.put("Perceived.Network.Size", String.format("%.2f", stats
                .getPNS()));
        result.put("Total.Exchanges", stats.getTotalExchanges() + "");

        return result;
    }

    public void writeGraph(String graphID, OutputStream out) throws Exception {
        if (graphID.equals("pns")) {
            Chart chart = new Chart("Perceived Network Size", "Time (seconds)",
                    "Perceived Network Size (nodes)");

            for (GossipAlgorithm algorithm : algorithms.values()) {
                algorithm.getStats().addPnsData(chart);
            }

            chart.writeImage(out, "png");
        } else if (graphID.equals("exchanges")) {
            Chart chart = new Chart("Total Number of Exchanges",
                    "Time (seconds)", "Number of Exchanges");

            for (GossipAlgorithm algorithm : algorithms.values()) {
                algorithm.getStats().addExchangesData(chart);
            }

            chart.writeImage(out, "png");
        } else {
            throw new Exception("unknown graph: " + graphID);
        }
    }
}
