package nl.vu.zorilla.net;

import ibis.util.ThreadPool;

import java.io.IOException;

import org.apache.log4j.Logger;
import smartsockets.Properties;
import smartsockets.direct.DirectServerSocket;
import smartsockets.direct.DirectSocket;
import smartsockets.direct.DirectSocketAddress;
import smartsockets.direct.DirectSocketFactory;
import smartsockets.util.TypedProperties;

import nl.vu.zorilla.Config;
import nl.vu.zorilla.Node;
import nl.vu.zorilla.NodeInfo;

public class Network implements Runnable {

    private static Logger logger = Logger.getLogger(Network.class);

    // service IDs
    public static final byte DISCOVERY_SERVICE = 1;

    public static final byte UDP_DISCOVERY_SERVICE = 2;

    public static final byte GOSSIP_SERVICE = 3;

    public static final byte VIVALDI_SERVICE = 4;

    public static final byte CLUSTER_SERVICE = 5;

    public static final byte FLOOD_SERVICE = 6;

    public static final byte JOB_SERVICE = 7;

    public static final byte WEB_SERVICE = 8;

    public static final byte ZONI_SERVICE = 9;

    // sockets et al.

    private final DirectSocketFactory socketFactory;

    private final DirectServerSocket serverSocket;

    private final Node node;

    public Network(Node node) throws IOException, Exception {
        this.node = node;

        TypedProperties factoryProperties = Properties.getDefaultProperties();

        String cluster = node.config().getProperty(Config.CLUSTER_NAME);

        if (cluster != null) {
            factoryProperties.put("smartsockets.networks.name", cluster);
        }

        socketFactory = DirectSocketFactory.getSocketFactory(factoryProperties);

        serverSocket = socketFactory.createServerSocket(
                node.config().getIntProperty(Config.PORT), 0, null);
        
    }

    public void start() {
        boolean firewalled = node.config().booleanProperty(Config.FIREWALL,
                false);
        if (!firewalled) {
            // start handling connections
            ThreadPool.createNew(this, "network connection handler");
        }
        logger.info("Started accepting connections on " + serverSocket.getAddressSet());
    }

    public void end() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            // IGNORE
        }
    }

    public DirectSocket connect(NodeInfo peer, byte serviceID, int timeout)
            throws IOException {
        DirectSocket result = socketFactory.createSocket(peer.getAddress(),
                timeout, 0, null);

        result.getOutputStream().write(serviceID);

        return result;
    }

    public DirectSocket connect(DirectSocketAddress address, byte serviceID,
            int timeout) throws IOException {
        DirectSocket result = socketFactory.createSocket(address, timeout, 0,
                null);

        result.getOutputStream().write(serviceID);

        return result;
    }

    public void run() {
        try {
            DirectSocket socket = serverSocket.accept();

            // create a new thread for the next connection
            ThreadPool.createNew(this, "network connection handler");

            byte service = (byte) socket.getInputStream().read();

            logger.debug("new connection received for service number: "
                    + service);

            switch (service) {
            case DISCOVERY_SERVICE:
                node.discoveryService().handleConnection(socket);
                break;
            case UDP_DISCOVERY_SERVICE:
                node.udpDiscoveryService().handleConnection(socket);
                break;
            case GOSSIP_SERVICE:
                node.gossipService().handleConnection(socket);
                break;
            case VIVALDI_SERVICE:
                node.vivaldiService().handleConnection(socket);
                break;
            case CLUSTER_SERVICE:
                node.clusterService().handleConnection(socket);
                break;
            case FLOOD_SERVICE:
                node.floodService().handleConnection(socket);
                break;
            case JOB_SERVICE:
                node.jobService().handleConnection(socket);
                break;
            case WEB_SERVICE:
                node.webService().handleConnection(socket);
                break;
            case ZONI_SERVICE:
                node.zoniService().handleConnection(socket);
                break;
            case -1:
                //the connection was closed
                break;
            default:
                logger.error("unknown service number in"
                        + " received message: " + service);
            }
        } catch (Throwable e) {
            if (!serverSocket.isClosed()) {
                logger.error("caught exception while handling connection", e);
            }
        }
    }

    public DirectSocketAddress getAddress() {
        return serverSocket.getAddressSet();
    }

}
