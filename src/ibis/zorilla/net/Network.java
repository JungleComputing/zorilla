package ibis.zorilla.net;

import ibis.util.ThreadPool;
import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import org.apache.log4j.Logger;

import ibis.io.Conversion;
import ibis.smartsockets.virtual.VirtualServerSocket;
import ibis.smartsockets.virtual.VirtualSocket;
import ibis.smartsockets.virtual.VirtualSocketAddress;
import ibis.smartsockets.virtual.VirtualSocketFactory;

public class Network implements Runnable {

    private static Logger logger = Logger.getLogger(Network.class);

    public static final int CONNECTION_TIMEOUT = 30 * 1000;

    public static final int VIRTUAL_PORT = 405;

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

    // node types

    public static final byte TYPE_NODE = 1;

    public static final byte TYPE_USER = 2;

    // sockets et al.

    private final VirtualSocketFactory socketFactory;

    private final VirtualServerSocket serverSocket;

    private final Node node;

    private final byte[] versionBytes;

    // map with smartsockets settings for light-weight connections
    private final HashMap<String, Object> lightConnection = new HashMap<String, Object>();

    public Network(Node node, Config properties, VirtualSocketFactory factory)
            throws IOException, Exception {
        this.node = node;

        socketFactory = factory;

        serverSocket = socketFactory.createServerSocket(VIRTUAL_PORT, 0, null);

        lightConnection.put("connect.module.allow", "ConnectModule(HubRouted)");

        // try {
        // ServiceLink sl = socketFactory.getServiceLink();
        // if (sl != null) {
        // // try to register...
        // if (!sl.registerProperty("smartsockets.viz",
        // "Z^Zorilla server:,"
        // + serverSocket.getLocalSocketAddress()
        // .toString())) {
        // // ...and update if it already exists
        // sl.updateProperty("smartsockets.viz", "Z^Zorilla node:,"
        // + serverSocket.getLocalSocketAddress().toString());
        // }
        // } else {
        // logger
        // .warn("could not set smartsockets viz property: could not get smartsockets service link");
        // }
        // } catch (Throwable e) {
        // logger.warn("could not register smartsockets viz property", e);
        // }

        // Create byte array out of version string. Use the fact that
        // It is made out of only numbers.
        long version = Node.getVersion();
        versionBytes = new byte[Long.SIZE];

        Conversion.defaultConversion.long2byte(version, versionBytes, 0);
    }

    public VirtualSocketFactory getSocketFactory() {
        return socketFactory;
    }

    public void start() {
        boolean firewalled = node.config().getBooleanProperty(Config.FIREWALL,
                false);
        if (!firewalled) {
            // start handling connections
            ThreadPool.createNew(this, "network connection handler");
        }
        logger.info("Started accepting connections on "
                + serverSocket.getLocalSocketAddress().machine());
    }

    public void end() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            // IGNORE
        }
        socketFactory.end();
    }

    public VirtualSocket connect(NodeInfo peer, byte serviceID)
            throws IOException {
        return connect(peer, serviceID, true);
    }

    public VirtualSocket connect(NodeInfo peer, byte serviceID,
            boolean fillTimeout) throws IOException {

        VirtualSocket result = socketFactory
                .createClientSocket(peer.getAddress(), CONNECTION_TIMEOUT,
                        fillTimeout, lightConnection);

        result.getOutputStream().write(TYPE_NODE);
        result.getOutputStream().write(versionBytes);
        result.getOutputStream().write(serviceID);
        result.getOutputStream().flush();

        return result;
    }

    public VirtualSocket connect(VirtualSocketAddress address, byte serviceID)
            throws IOException {
        return connect(address, serviceID, true);
    }

    public VirtualSocket connect(VirtualSocketAddress address, byte serviceID,
            boolean fillTimeout) throws IOException {
        VirtualSocket result = socketFactory.createClientSocket(address,
                CONNECTION_TIMEOUT, fillTimeout, lightConnection);

        result.getOutputStream().write(TYPE_NODE);
        result.getOutputStream().write(versionBytes);
        result.getOutputStream().write(serviceID);
        result.getOutputStream().flush();

        return result;
    }

    public void run() {
        VirtualSocket socket = null;

        try {
            socket = serverSocket.accept();
        } catch (Exception e) {
            if (!serverSocket.isClosed()) {
                logger.error("caught exception while handling connection", e);
            }
        }

        // create a new thread for the next connection
        ThreadPool.createNew(this, "network connection handler");

        if (socket == null) {
            return;
        }

        try {
            byte service;

            InputStream in = socket.getInputStream();

            byte type = (byte) in.read();

            if (type == TYPE_NODE) {

                for (int i = 0; i < versionBytes.length; i++) {
                    if (versionBytes[i] != (byte) in.read()) {
                        throw new IOException(
                                "remote version of node not equal to version");
                    }
                }

                service = (byte) in.read();

            } else if (type == TYPE_USER) {
                service = ZONI_SERVICE;
            } else {
                throw new IOException("unknown connection type: " + type);
            }

            logger.debug("new connection received for service number: "
                    + service + " from node type " + type);

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
                // the connection was closed
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

    public VirtualSocketAddress getAddress() {
        return serverSocket.getLocalSocketAddress();
    }

}
