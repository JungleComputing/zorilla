package nl.vu.zorilla.bigNet.bamboo;

import ibis.util.IPUtils;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.UUID;

import org.apache.log4j.Logger;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaException;
import nl.vu.zorilla.bigNet.Message;
import nl.vu.zorilla.bigNet.Metric;
import nl.vu.zorilla.bigNet.Network;

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
            inetAddress = IPUtils.getLocalHostAddress();
        }

        int port = node.config().getPort();

        if (port == 0) {
            port = getFreePort();
        }

        socketAddress = new InetSocketAddress(inetAddress, port);

        logger.debug("initializing network");

        id = node.getID();

        logger.debug("initializing bamboo");

        node.message("Please ignore JNI warning and Sandstorm message printed by Bamboo");
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
                udpBroadcastTransport = new UdpBroadcastTransport(this, broadcastPort, node);
                node.message("Started UDP broadcast node discovery on port " + broadcastPort);
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

    // private void send(BambooMessage message, Address destination, UUID jobID,
    // Function function) throws NetworkException, ZorillaException {
    //
    // if (destination == null || destination.unknown()) {
    // throw new ZorillaException("destination address not given");
    // }
    //
    // message.flip();
    //
    // message.setType(MessageType.USER);
    // message.setID(Node.generateUUID());
    // message.setSource(nodeAddress());
    // message.setDestination(destination);
    // message.setFunction(function);
    // message.setJobID(jobID);
    //
    // bambooTransport.send(message);
    // }

    // private void send(BambooMessage message, Address[] destinations, UUID
    // jobID,
    // Function function) throws NetworkException, ZorillaException {
    // for (int i = 0; i < destinations.length; i++) {
    // send(message.copy(), destinations[i], jobID, function);
    // }
    // BambooMessage.recycle(message);
    // }
    //
    // private BambooMessage call(BambooMessage message, Address destination,
    // UUID jobID,
    // Function function, long deadline) throws NetworkException,
    // ZorillaException {
    // UUID callID = Node.generateUUID();
    //
    // message.flip();
    // message.setType(MessageType.CALL);
    // message.setID(callID);
    // message.setSource(nodeAddress());
    // message.setDestination(destination);
    // message.setJobID(jobID);
    // message.setFunction(function);
    //
    // Call call = new Call(message, this, deadline);
    //
    // pendingCalls.put(callID, call);
    //
    // return call.call();
    // }

    public void flood(Message m, Metric metric, long metricValue)
            throws ZorillaException {
        if (metric != Metric.LATENCY_HOPS) {
            throw new ZorillaException(metric
                    + " flooding not supported by Bamboo network layer");
        }
        BambooMessage message = (BambooMessage) m;

        message.flip();
        message.setID(Node.generateUUID());
        message.setType(MessageType.USER);
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

    // /**
    // * called by the message and call handlers
    // */
    // private void send(BambooMessage message) {
    // if (logger.isDebugEnabled()) {
    // logger.debug("sending: " + message.toVerboseString());
    // }
    // message.setSource(nodeAddress());
    // bambooTransport.send(message);
    // }

    // called from the transports
    void messageReceived(BambooMessage m) {
        if (logger.isDebugEnabled()) {
            logger.debug("receiving message: " + m.toVerboseString());
        }

        switch (m.getType()) {
        case USER:
            new MessageHandler(m, node);
            break;
        default:
            assert false : "invalid message type";
        }
    }

    public Message getMessage() {
        return BambooMessage.getMessage();
    }

    public InetSocketAddress address() {
        return socketAddress;
    }

}
