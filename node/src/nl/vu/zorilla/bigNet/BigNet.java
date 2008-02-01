package nl.vu.zorilla.bigNet;

import ibis.util.IPUtils;
import ibis.util.ThreadPool;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import smartsockets.Properties;
import smartsockets.direct.DirectServerSocket;
import smartsockets.direct.DirectSocket;
import smartsockets.direct.DirectSocketFactory;
import smartsockets.direct.SocketAddressSet;
import smartsockets.util.TypedProperties;

import nl.vu.zorilla.JobAdvert;
import nl.vu.zorilla.Network;
import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaException;
import nl.vu.zorilla.bigNet.gossip.GossipService;
import nl.vu.zorilla.stats.Stats;
import nl.vu.zorilla.util.SizeOf;

public class BigNet extends Network {

    private static Logger logger = Logger.getLogger(BigNet.class);

    // service IDs

    public static final byte DISCOVERY_SERVICE = 1;

    public static final byte PING_SERVICE = 2;

    public static final byte GOSSIP_SERVICE = 3;

    public static final byte NEIGHBOUR_SERVICE = 4;

    public static final byte MESSAGE_SERVICE = 5;

    // service

    private final DiscoveryService discoveryService;

    private final PingService pingService;

    private final GossipService gossipService;

    private final NeighbourService neighbourService;

    private final MessageService messageService;

    // sockets et al.

    private final DirectSocketFactory socketFactory;

    private final DirectServerSocket serverSocket;

    private final SocketAddressSet addresses;

    private final SocketAddress udpAddress;

    private final DatagramSocket datagramSocket;

    private final Node node;

    private final String nodeName;

    private Coordinates coordinates;

    private class MessageHandler implements Runnable {
        MessageHandler() {
            node.message("P2P node UDP address: " + udpAddress);

            ThreadPool.createNew(this, "message handler");
        }

        public void run() {
            Thread.currentThread().setPriority(6);
            byte[] buffer = new byte[SizeOf.UDP_MAX_PAYLOAD_LENGTH];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            while (true) {
                try {
                    datagramSocket.receive(packet);

                    if (packet.getLength() != 0) {

                        byte service = buffer[0];

                        logger.debug("message received for service number: "
                                + service);

                        switch (service) {
                        case DISCOVERY_SERVICE:
                            discoveryService.handleMessage(packet);
                            break;
                        case PING_SERVICE:
                            pingService.handleMessage(packet);
                            break;
                        case GOSSIP_SERVICE:
                            gossipService.handleMessage(packet);
                            break;
                        case NEIGHBOUR_SERVICE:
                            neighbourService.handleMessage(packet);
                            break;
                        case MESSAGE_SERVICE:
                            messageService.handleMessage(packet);
                            break;
                        default:
                            logger
                                    .error("unknown service number in received message: "
                                            + service);
                        }

                    }

                } catch (IOException e) {
                    logger.error("caught exception while "
                            + "recieving message ", e);
                }
            }
        }
    }

    private class ConnectionHandler implements Runnable {
        ConnectionHandler() {
            node.message("P2P node TCP address: " + addresses);

            ThreadPool.createNew(this, "connection handler");
        }

        public void run() {
            try {
                DirectSocket socket = serverSocket.accept();

                // create a new thread for the next connection
                ThreadPool.createNew(this, "connection handler");

                byte service = (byte) socket.getInputStream().read();

                logger.debug("new connection received for service number: "
                        + service);

                switch (service) {
                case DISCOVERY_SERVICE:
                    discoveryService.handleConnection(socket);
                    break;
                case PING_SERVICE:
                    pingService.handleConnection(socket);
                    break;
                case GOSSIP_SERVICE:
                    gossipService.handleConnection(socket);
                    break;
                case NEIGHBOUR_SERVICE:
                    neighbourService.handleConnection(socket);
                    break;
                case MESSAGE_SERVICE:
                    messageService.handleConnection(socket);
                    break;

                default:
                    logger.error("unknown service number in"
                            + " received message: " + service);
                }

            } catch (IOException e) {
                logger.error("caught exception while handling connection",
                        e);
            }
        }
    }

    public BigNet(Node node) throws IOException, ZorillaException {
        this.node = node;

        coordinates = new Coordinates();

        TypedProperties factoryProperties = Properties.getDefaultProperties();

        String cluster = node.config().getCluster();

        if (cluster != null) {
            factoryProperties.put("smartsockets.networks.name", cluster);
        }

        socketFactory = DirectSocketFactory.getSocketFactory(factoryProperties);

        serverSocket = socketFactory.createServerSocket(
                node.config().getPort(), 0, null);
        addresses = serverSocket.getAddressSet();

        node
                .message("node P2P network running on "
                        + addresses.getAddressSet());

        datagramSocket = new DatagramSocket(node.config().getPort());

        // FIXME: find out the _real_ address OR get rid of UDP
        udpAddress = new InetSocketAddress(IPUtils.getLocalHostAddress(),
                datagramSocket.getLocalPort());

        if (cluster == null) {
            nodeName = InetAddress.getLocalHost().getHostName()
                    + ":"
                    + serverSocket.getAddressSet().getSocketAddresses()[0]
                            .getPort();

        } else {
            nodeName = InetAddress.getLocalHost().getHostName()
                    + ":"
                    + serverSocket.getAddressSet().getSocketAddresses()[0]
                            .getPort() + "@" + cluster;
        }

        boolean firewalled = node.getProperties().booleanProperty(
                "zorilla.bignet.firewalled", false);

        LocalNetWatch localNetWatch = null;
        if (node.config().getBroadcastPort() == 0) {
            node.message("port 0 specified for local node discovery,"
                    + " disabling");
        } else if (firewalled) {
            node.message("node is firewalled and will "
                    + "not accept messages and connections");
        } else {

            try {
                localNetWatch = new LocalNetWatch(node.config()
                        .getBroadcastPort(), this);
                node.message("Started local node discovery on port "
                        + localNetWatch.getPort());
            } catch (Exception e) {
                node.warn("error on starting local node discovery, disabling",
                        e);
            }
        }

        String peers = node.getProperties().getProperty("zorilla.peers");

        if (peers == null) {
            discoveryService = new DiscoveryService(this, new String[0]);
            ;
        } else {
            discoveryService = new DiscoveryService(this, peers.split(","));
        }

        // pingService = new UDPPingService(this);
        pingService = new TCPPingService(this);

        gossipService = new GossipService(this);

        // single list neighbourhood
        // neighbourService = new SimpleNeighbourhood(this);

        neighbourService = new NullNeighbourService(this);

        messageService = new MessageService(this, node);

        if (localNetWatch != null) {
            localNetWatch.start();
        }
        discoveryService.start();
        pingService.start();
        gossipService.start();
        neighbourService.start();
        messageService.start();

        if (!firewalled) {
            // start some threads for handling messages and connections
            new MessageHandler();
            new ConnectionHandler();
        }

        logger.debug("done initializing network");
    }

    @Override
    public String getNodeName() {
        return nodeName;
    }

    synchronized Coordinates getCoordinates() {
        return coordinates;
    }

    public NodeInfo getNodeInfo() {
        return new NodeInfo(node.getID(), nodeName, getCoordinates(),
                addresses, udpAddress);
    }

    public void send(byte[] data, SocketAddress receiver) throws IOException {
        DatagramPacket packet = new DatagramPacket(data, data.length, receiver);

        datagramSocket.send(packet);
    }

    void send(byte[] data, NodeInfo receiver) throws IOException {
        send(data, receiver.getUdpAddress());
    }

    public DirectSocket connect(NodeInfo peer, byte serviceID, int timeout)
            throws IOException {
        DirectSocket result = socketFactory.createSocket(peer.getAddress(),
                timeout, 0, new HashMap());

        result.getOutputStream().write(serviceID);

        return result;
    }

    DirectSocket connect(SocketAddressSet address, byte serviceID, int timeout)
            throws IOException {
        DirectSocket result = socketFactory.createSocket(address, timeout, 0,
                new HashMap());

        result.getOutputStream().write(serviceID);

        return result;
    }

    NodeInfo[] getGossipCache() {
        return gossipService.getNodes();
    }

    public UUID getNodeID() {
        return node.getID();
    }

    NodeInfo[] getRandomNodes(int n) {
        return gossipService.selectRandom(n);
    }

    public double doPing(NodeInfo peer) throws IOException {
        return pingService.ping(peer);
    }

    public void getStats(Stats stats) {
        Map<String, Object> result = new HashMap<String, Object>();

        result.put("gossip.nodes", gossipService.getNodes().length);
        result.put("neighbours", neighbourService.getNodes().length);

        result.put("coordinates", getCoordinates());

        stats.put("network", result);
    }

    void newNode(NodeInfo info) {
        logger.info("new node detected: " + info);
        gossipService.newNode(info);
        neighbourService.newNode(info);
    }

    @Override
    public void advertise(JobAdvert advert) throws IOException,
            ZorillaException {
        messageService.advertise(advert);
    }

    @Override
    public void killNetwork() throws ZorillaException {
        messageService.killNetwork();
    }

    public void end(long deadline) {
        gossipService.end();
    }

    NodeInfo[] getNeighbours() {
        return neighbourService.getNodes();
    }

    double distanceToClosestNeighbour() {
        return neighbourService.distanceToClosestNode();
    }

    synchronized void updateCoordinates(Coordinates remoteCoordinates,
            double rtt) {
        coordinates = coordinates.update(remoteCoordinates, rtt);
    }

    @Override
    public InetAddress guessInetAddress() {
        if (addresses.getAddressSet().getAddresses().length == 0) {
            return null;
        }
        return addresses.getAddressSet().getAddresses()[0];
    }

    public File getHomeDir() {
        return node.config().getHomeDir();
    }

}
