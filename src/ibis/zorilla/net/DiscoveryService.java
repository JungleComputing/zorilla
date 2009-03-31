package ibis.zorilla.net;

import ibis.smartsockets.virtual.VirtualSocket;
import ibis.smartsockets.virtual.VirtualSocketAddress;
import ibis.util.ThreadPool;
import ibis.zorilla.ZorillaTypedProperties;
import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;
import ibis.zorilla.Service;
import ibis.zorilla.zoni.ZoniProtocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

public class DiscoveryService implements Service, Runnable {

    // how long do we wait for a connection (10 seconds)
    public static final int CONNECT_TIMEOUT = 10 * 1000;

    // how often do we re-do the discovery process to update node info? (5 min)
    public static final int DISCOVERY_INTERVAL = 5 * 60 * 1000;

    public static final int MAX_TRIES = 5;

    private static final Logger logger = Logger
            .getLogger(DiscoveryService.class);

    private final VirtualSocketAddress[] addresses;

    private final Node node;

    private final Map<UUID, NodeInfo> nodes;

    public DiscoveryService(Node node) throws IOException {
        this.node = node;

        nodes = new HashMap<UUID, NodeInfo>();

        Set<VirtualSocketAddress> addresses = new HashSet<VirtualSocketAddress>();

        for (String string : node.config().getStringList(
                ZorillaTypedProperties.PEERS)) {
            try {
                try {
                    addresses.add(new VirtualSocketAddress(string));
                } catch (Exception e) {
                    addresses.add(new VirtualSocketAddress(string,
                            ZoniProtocol.VIRTUAL_PORT));
                }
            } catch (Exception e) {
                logger.warn("invalid peer address: " + string, e);
            }
        }

        // also add master (if available)
        String masterAddress = node.config().getProperty(
                ZorillaTypedProperties.MASTER_ADDRESS);
        if (node.config().isWorker() && masterAddress != null) {
            try {
                addresses.add(new VirtualSocketAddress(masterAddress));
            } catch (Exception e) {
                logger.warn("invalid master address: " + masterAddress, e);
            }

        }

        this.addresses = addresses.toArray(new VirtualSocketAddress[0]);
    }

    public void start() {
        ThreadPool.createNew(this, "discovery");
        logger.info("Started Discovery service");
    }

    public NodeInfo[] getNodesList() {
        return nodes.values().toArray(new NodeInfo[0]);
    }

    private void doRequests(VirtualSocketAddress peer) {
        for (int i = 0; i < MAX_TRIES; i++) {
            try {
                NodeInfo info = doRequest(peer);
                synchronized (this) {
                    nodes.put(info.getID(), info);
                }

                return;
            } catch (Exception e) {
                logger.error("Lookup of peer: " + peer + " failed", e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // IGNORE
            }
        }
    }

    private NodeInfo doRequest(VirtualSocketAddress address) throws IOException {
        logger.debug("sending lookup request to " + address);

        VirtualSocket socket = node.network().connect(address,
                Network.DISCOVERY_SERVICE, CONNECT_TIMEOUT);

        logger.debug("connected!");

        socket.getOutputStream().write(42);
        socket.getOutputStream().flush();

        try {
            ObjectInputStream in = new ObjectInputStream(socket
                    .getInputStream());
            NodeInfo reply = (NodeInfo) in.readObject();
            socket.close();

            logger.debug("reply received");
            return reply;
        } catch (ClassNotFoundException e) {
            throw new IOException("could not read reply object: " + e);
        }
    }

    public void handleConnection(VirtualSocket socket) {
        logger.debug("handling connection from "
                + socket.getRemoteSocketAddress());

        try {
            // read "request" byte
            socket.getInputStream().read();

            ObjectOutputStream out = new ObjectOutputStream(socket
                    .getOutputStream());
            out.writeObject(node.getInfo());
            out.flush();

            socket.close();

        } catch (Exception e) {
            logger.error("error on handling request", e);
            try {
                socket.close();
            } catch (IOException e2) {
                // IGNORE
            }
        }
    }

    public void run() {
        while (true) {
            for (VirtualSocketAddress peer : addresses) {
                doRequests(peer);
            }

            try {
                Thread.sleep(DISCOVERY_INTERVAL);
            } catch (InterruptedException e) {
                // IGNORE
            }
        }
    }

    public synchronized Map<String, String> getStats() {
        Map<String, String> result = new HashMap<String, String>();

        return result;

    }
}
