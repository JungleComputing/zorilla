package ibis.zorilla.net;

import ibis.smartsockets.virtual.VirtualSocket;
import ibis.smartsockets.virtual.VirtualSocketAddress;
import ibis.util.ThreadPool;
import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;
import ibis.zorilla.Service;

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
    
    public static final int VIRTUAL_PORT = 405;

    private static final Logger logger = Logger
            .getLogger(DiscoveryService.class);

    private final Set<VirtualSocketAddress> addresses;

    private final Node node;

    private final Map<UUID, NodeInfo> nodes;

    public DiscoveryService(Node node) throws IOException {
        this.node = node;

        nodes = new HashMap<UUID, NodeInfo>();

        addresses = new HashSet<VirtualSocketAddress>();

        for (String string : node.config().getStringList(Config.PEERS)) {
            addPeer(string);
        }
    }

    public synchronized void addPeer(String address) {
        VirtualSocketAddress socketAddress = null;
        try {
            try {
                socketAddress = new VirtualSocketAddress(address);
            } catch (Exception e) {
                socketAddress = new VirtualSocketAddress(address,
                        VIRTUAL_PORT);
            }
        } catch (Exception e) {
            logger.warn("invalid peer address: " + address, e);
        }
        if (socketAddress != null) {
            addresses.add(socketAddress);

            // nudge the discovery thread
            notifyAll();

            // register hub at smartsockets too
            node.getIPLServer().addHubs(socketAddress.machine());
        }
    }

    private synchronized VirtualSocketAddress[] getAddresses() {
        return addresses.toArray(new VirtualSocketAddress[0]);
    }

    public void start() {
        ThreadPool.createNew(this, "discovery");
        logger.info("Started Discovery service");
    }

    public NodeInfo[] getNodesList() {
        return nodes.values().toArray(new NodeInfo[0]);
    }

    private void doRequests(VirtualSocketAddress peer) {
        Exception exception = null;
        for (int i = 0; i < MAX_TRIES; i++) {
            try {
                NodeInfo info = doRequest(peer);
                synchronized (this) {
                    nodes.put(info.getID(), info);
                }
                if (info.isHub()) {
                    // register hub
                    node.getIPLServer().addHubs(info.getAddress().machine());
                }
                return;
            } catch (Exception e) {
                exception = e;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // IGNORE
            }
        }
        logger.warn("Lookup of peer: " + peer + " failed after " + MAX_TRIES
                + " tries", exception);
    }

    private NodeInfo doRequest(VirtualSocketAddress address) throws IOException {
        logger.debug("sending lookup request to " + address);

        VirtualSocket socket = node.network().connect(address,
                Network.DISCOVERY_SERVICE);

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
            for (VirtualSocketAddress peer : getAddresses()) {
                doRequests(peer);
            }

            synchronized (this) {
                try {
                    wait(DISCOVERY_INTERVAL);
                } catch (InterruptedException e) {
                    // IGNORE
                }
            }
        }
    }

    public synchronized Map<String, String> getStats() {
        Map<String, String> result = new HashMap<String, String>();

        return result;

    }

}
