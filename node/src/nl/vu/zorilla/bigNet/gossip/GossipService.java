package nl.vu.zorilla.bigNet.gossip;

import ibis.util.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import nl.vu.zorilla.bigNet.BigNet;
import nl.vu.zorilla.bigNet.NodeInfo;
import nl.vu.zorilla.bigNet.Service;
import nl.vu.zorilla.util.SizeOf;

import org.apache.log4j.Logger;

import smartsockets.direct.DirectSocket;

public class GossipService implements Service, Runnable {
    
    public static final double MESSAGE_LOSS_FRACTION  = 0.2;

    public static final int TIMEOUT = 10 * 1000;
    public static final int TIMEOUT_MARGIN = 5 * 1000;

    public static final int STATS_DELAY = 0 * TIMEOUT;
    
    public static final int BOOTSTRAP_DELAY = 100 * 1000; 

    private static final Logger logger = Logger.getLogger(GossipService.class);

    private final BigNet network;

    private Map<String, GossipAlgorithm> algorithms;

    private GossipAlgorithm mainAlgorithm;

    private final Stats stats;
    
    private final GossipCache bootstrapCache;
    private boolean bootstrap = true;
    
    private Random random;

    public GossipService(BigNet network) {
        this.network = network;

        algorithms = new HashMap<String, GossipAlgorithm>();

        GossipAlgorithm cyclon = new Cyclon("Cyclon", true, false, false, this);
        algorithms.put(cyclon.getName(), cyclon);

        
        GossipAlgorithm retryCyclon = new Cyclon("RetryCyclon", true, true, false, this);
        algorithms.put(retryCyclon.getName(), retryCyclon);

        GossipAlgorithm fallbackCyclon = new Cyclon("FallbackCyclon", true, true, true, this);
        algorithms.put(fallbackCyclon.getName(), fallbackCyclon);

        GossipAlgorithm randomGossip = new RandomGossip("RandomGossip",
                true, false, false, this);
        algorithms.put(randomGossip.getName(), randomGossip);

        GossipAlgorithm retryRandomGossip = new RandomGossip("RetryRandomGossip",
                true, true, false, this);
        algorithms.put(retryRandomGossip.getName(), retryRandomGossip);

        GossipAlgorithm fallbackRandomGossip = new RandomGossip("FallbackRandomGossip",
                true, true, true, this);
        algorithms.put(fallbackRandomGossip.getName(), fallbackRandomGossip);

        mainAlgorithm = fallbackRandomGossip;
        
        bootstrapCache = new GossipCache();

        stats = new Stats(network.getHomeDir(), algorithms.keySet(),
                STATS_DELAY);

        random = new Random();
    }

    public void start() {
        for (GossipAlgorithm algorithm: algorithms.values()) {
            algorithm.start();
        }
        ThreadPool.createNew(this, "gossip service");
    }

    public void handleMessage(DatagramPacket packet) {
        try {
            InputStream bytes = new ByteArrayInputStream(packet.getData());
            bytes.skip(1);
            ObjectInputStream in = new ObjectInputStream(bytes);

            GossipMessage request = (GossipMessage) in.readObject();
            in.close();

            logger.debug("got gossip request from " + request.getSender());

            GossipAlgorithm algorithm = algorithms.get(request
                    .getAlgorithmName());

            if (algorithm == null) {
                throw new IOException("could not find algorithm: "
                        + request.getAlgorithmName());
            }

            stats.addToStats(request);

            // implemented by sub class
            GossipMessage reply = algorithm.handleRequest(request);

            if (!request.replyRequested()) {
                return;
            }

            logger.debug("sending reply");

            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byteOut);

            out.writeObject(reply);
            out.close();

            byte[] data = byteOut.toByteArray();

            network.send(data, packet.getSocketAddress());

            logger.debug("reply send");

        } catch (IOException e) {
            logger.error("error on handling gossip message", e);
        } catch (ClassNotFoundException e) {
            logger.error("error on handling gossip message", e);
        }
    }

    GossipMessage doUdpRequest(GossipMessage request, long timeout)
            throws IOException {
        try {
            byte[] buffer = new byte[SizeOf.UDP_MAX_PAYLOAD_LENGTH];

            DatagramSocket socket = new DatagramSocket();
            socket.setSoTimeout((int) timeout);

            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            byteOut.write(BigNet.GOSSIP_SERVICE);

            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(request);

            out.close();

            byte[] data = byteOut.toByteArray();
            DatagramPacket packet = new DatagramPacket(data, data.length,
                    request.getReceiver().getUdpAddress());

            logger.debug("gossiping with " + request.getReceiver());

            socket.send(packet);

            if (!request.replyRequested()) {
                return null;
            }

            packet.setData(buffer);

            // wait for reply
            socket.receive(packet);
            socket.close();

            ObjectInputStream in = new ObjectInputStream(
                    new ByteArrayInputStream(buffer));

            GossipMessage reply = (GossipMessage) in.readObject();

            logger.debug("got gossip reply");

            stats.addToStats(reply);

            return reply;

        } catch (ClassNotFoundException e) {
            throw new IOException("error on reading object: " + e);
        }
    }

    GossipMessage doTcpRequest(GossipMessage request, int timeout)
            throws IOException {
        
        if (random.nextDouble() < MESSAGE_LOSS_FRACTION) {
            throw new IOException("Request message lost! (at random)");
        }
        
        try {
            logger.debug("doing tcp request to " + request.getReceiver());
            DirectSocket socket = network.connect(request.getReceiver(),
                    BigNet.GOSSIP_SERVICE, timeout);
            
            ObjectOutputStream out = new ObjectOutputStream(socket
                    .getOutputStream());
            out.writeObject(request);
            out.flush();
            
            logger.debug("send request, receiving reply");

            ObjectInputStream in = new ObjectInputStream(socket
                    .getInputStream());
            GossipMessage reply = (GossipMessage) in.readObject();
            socket.close();
            
            logger.debug("reply received");

            stats.addToStats(reply);
            
            return reply;
        } catch (ClassNotFoundException e) {
            throw new IOException("could not read reply object: " + e);
        }
    }

    public void handleConnection(DirectSocket socket) {
        try {
            ObjectInputStream in = new ObjectInputStream(socket
                    .getInputStream());
            GossipMessage request = (GossipMessage) in.readObject();

            GossipAlgorithm algorithm = algorithms.get(request
                    .getAlgorithmName());

            if (algorithm == null) {
                throw new IOException("could not find algorithm: "
                        + request.getAlgorithmName());
            }

            stats.addToStats(request);

            GossipMessage reply = algorithm.handleRequest(request);

            ObjectOutputStream out = new ObjectOutputStream(socket
                    .getOutputStream());
            
            if (random.nextDouble() < MESSAGE_LOSS_FRACTION) {
                logger.warn("warning, loosing message");
                socket.close();
            } else {
                out.writeObject(reply);
                out.flush();

                socket.close();

            }

        } catch (Exception e) {
            logger.error("error on handling request", e);
            try {
                socket.close();
            } catch (IOException e2) {
                // IGNORE
            }
        }
    }

    public NodeInfo[] getNodes() {
        return mainAlgorithm.getNodes();
    }

    //called by bootstrap algorithms
    public synchronized void newNode(NodeInfo info) {
        if (!bootstrap) {
            return;
        }
        bootstrapCache.add(new GossipCacheEntry(info));
    }
    
    /**
     * called by algorithms when they run out of entries
     * @return a random entry from the bootstrap cache
     */
    public NodeInfo getBootstrapNode() {
        return bootstrapCache.selectRandom();
    }

    public NodeInfo[] selectRandom(int n) {
        return mainAlgorithm.getRandom(n);
    }

    public NodeInfo getNodeInfo() {
        return network.getNodeInfo();
    }
    
    Stats  getStats() {
        return stats;
    }
    
    public void end() {
        stats.printCharts();
    }
    
    public synchronized void run() {
        try {
            wait(BOOTSTRAP_DELAY);
        } catch (InterruptedException e) {
            //IGNORE
        }
        bootstrapCache.clear();
        bootstrap = false;
    }
}