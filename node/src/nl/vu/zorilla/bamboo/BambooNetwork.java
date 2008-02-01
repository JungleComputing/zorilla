package nl.vu.zorilla.bamboo;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.UUID;

import org.apache.log4j.Logger;

import smartsockets.direct.IPAddressSet;

import nl.vu.zorilla.JobAdvert;
import nl.vu.zorilla.Network;
import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaException;
import nl.vu.zorilla.stats.Stats;

public final class BambooNetwork extends Network {

    private static final Logger logger = Logger.getLogger(BambooNetwork.class);

    private final Node node;

    private final UUID id;

    private final BambooTransport bambooTransport;

    private final UdpBroadcastTransport udpBroadcastTransport;

    private final InetSocketAddress socketAddress;

    public BambooNetwork(Node node) throws Exception {
        this.node = node;

        InetAddress inetAddress = node.config().getNodeAddress();

        if (inetAddress == null) {
            inetAddress = guessInetAddress();
        }

        int port = node.config().getPort();

        if (port == 0) {
            port = getFreePort();
        }

        socketAddress = new InetSocketAddress(inetAddress, port);

        logger.debug("initializing network");

        id = node.getID();

        logger.debug("initializing bamboo");

        node
                .message("Please ignore JNI warning and Sandstorm message printed by Bamboo");
        bambooTransport = new BambooTransport(node, this, socketAddress);

        logger.debug("done initializing bamboo");

        node.message("Started Bamboo P2P network peer on " + address());

        Address nodeAddress = new Address(node, id, bambooTransport.location(),
                socketAddress, Address.Type.NORMAL, node.config().getCluster());

        UdpBroadcastTransport udpBroadcastTransport = null;
        int broadcastPort = node.config().getBroadcastPort();
        if (broadcastPort == 0) {
            node.message("UDP broadcast node discovery disabled");
        } else {
            try {
                udpBroadcastTransport = new UdpBroadcastTransport(this,
                        broadcastPort, node);
                node.message("Started UDP broadcast node discovery on port "
                        + broadcastPort);
            } catch (BindException e) {
                node.warn("Disabling UDP broadcast node discovery,"
                        + " could not bind socket: ", e);
            }
        }
        this.udpBroadcastTransport = udpBroadcastTransport;

        logger.debug("node address:\n" + nodeAddress.toVerboseString());
    }

    /**
     * Evil hack to get a free port. creates a server port, and closes it again
     * (remembering the number) No guarantee this actually works...
     * 
     * @throws IOException
     */
    private static int getFreePort() throws ZorillaException {
        try {
            ServerSocket socket = new ServerSocket(0);

            int result = socket.getLocalPort();

            socket.close();

            return result;
        } catch (IOException e) {
            throw new ZorillaException("cannot allocate a free port for the"
                    + " bamboo network", e);
        }
    }

    void flood(BambooMessage m, Metric metric, long metricValue)
            throws ZorillaException {
        if (metric != Metric.LATENCY_HOPS) {
            throw new ZorillaException(metric
                    + " flooding not supported by Bamboo network layer");
        }
        BambooMessage message = (BambooMessage) m;

        message.flip();
        message.setID(Node.generateUUID());
        message.setType(MessageType.JOB_ADVERT);
        message.setTtl((int) metricValue);
        message.setSource(nodeAddress());
        message.setDestination(Address.UNKNOWN);

        bambooTransport.flood(message);
    }

    // private Address nodeLocation() {
    // return new Address(bambooTransport.location(), node);
    // }

    public void end(long deadline) {
        if (bambooTransport != null) {
            bambooTransport.kill();
        }

        if (udpBroadcastTransport != null) {
            udpBroadcastTransport.kill();
        }

    }

    void newLocalNode(Address info) {
        logger.debug("new local node detected");

        if (bambooTransport != null) {
            bambooTransport.newLocalNode(info);
        }
    }

    synchronized Address nodeAddress() {
        return new Address(node, id, bambooTransport.location(), socketAddress,
                Address.Type.NORMAL, node.config().getCluster());
    }

    // called from the transports
    void messageReceived(BambooMessage m) {
        if (logger.isDebugEnabled()) {
            logger.debug("receiving message: " + m.toVerboseString());
        }
        
        new MessageHandler(m, node);
    }

    BambooMessage getMessage() {
        return BambooMessage.getMessage();
    }

    InetSocketAddress address() {
        return socketAddress;
    }

    @Override
    public void advertise(JobAdvert advert) throws IOException, ZorillaException {
        BambooMessage message = getMessage();
        message.setFunction(Function.JOB_ADVERT);
        message.writeObject(advert);
        flood(message, Metric.LATENCY_HOPS, advert.getCount());
    }

    @Override
    public void killNetwork() throws ZorillaException {
        BambooMessage message = getMessage();
        message.setFunction(Function.KILL_NETWORK);
        flood(message, Metric.LATENCY_HOPS, 100);
    }

    @Override
    public String getNodeName() {
        return address().getHostName() + ":" + address().getPort();
    }

    public InetAddress guessInetAddress() {
        InetAddress result = IPAddressSet.getLocalHost().getAddresses()[0];
        logger.debug("local address is " + result);
        return result;
    }
    
    @Override
    public void getStats(Stats stats) {
        return;
    }

}
