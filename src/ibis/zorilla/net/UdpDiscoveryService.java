package ibis.zorilla.net;

import ibis.util.ThreadPool;
import ibis.zorilla.Config;
import ibis.zorilla.Node;
import ibis.zorilla.NodeInfo;
import ibis.zorilla.Service;
import ibis.zorilla.io.ByteBufferInputStream;
import ibis.zorilla.io.ByteBufferOutputStream;
import ibis.zorilla.util.SizeOf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;


import org.apache.log4j.Logger;

import ibis.smartsockets.direct.DirectSocket;

/**
 * Maintains a list of all nodes reachable through UDP broadcast (the
 * neighbourhood of this node)
 */
public final class UdpDiscoveryService implements Runnable, Service {

    public static final int NODE_ANNOUNCE = 0;

    public static final int NODE_ANNOUNCE_REQUEST = 1;

    public static final int NODE_ANNOUNCE_REPLY = 2;

    // timeout before we send another broadcast
    public static final long TIMEOUT = 5 * 60 * 1000; // 5 minutes

    // time after which an entry expires(25 minutes)
    public static final long NEIGHBOUR_LIFETIME = 25 * 60 * 1000;

    private static final Logger logger = Logger
            .getLogger(UdpDiscoveryService.class);

    private final DatagramChannel channel;

    private final InetSocketAddress sendAddress;

    private final Selector selector;

    private final Node node;

    private final HashMap<UUID, LocalNode> knownNodes;

    private static class LocalNode {
        private long expirationTime;

        private NodeInfo node;

        LocalNode(NodeInfo node) {
            this.node = node;

            reset();
        }

        void reset() {
            expirationTime = System.currentTimeMillis() + NEIGHBOUR_LIFETIME;
        }

        boolean isExpired() {
            return System.currentTimeMillis() > expirationTime;
        }

        NodeInfo getNode() {
            return node;
        }
    }

    public UdpDiscoveryService(Node node) throws IOException {
        this.node = node;

        knownNodes = new HashMap<UUID, LocalNode>();

        int port = node.config().getIntProperty(Config.PORT);

        if (port == 0) {
            logger.info(Config.PORT
                    + " set to 0, Disabling UDP Discovery service");
            channel = null;
            selector = null;
            sendAddress = null;
            return;
        }

        channel = DatagramChannel.open();

        sendAddress = new InetSocketAddress(InetAddress
                .getByName("255.255.255.255"), port);

        channel.socket().bind(new InetSocketAddress(port));
        channel.socket().setBroadcast(true);

        selector = Selector.open();

        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_READ);
    }

    public void start() {
        if (channel == null) {
            return;
        }

        ThreadPool.createNew(this, "neighbourhood watch");
        logger.info("Started UDP Discovery service on port "
                + channel.socket().getLocalPort());

    }

    public int getPort() {
        if (channel == null) {
            return 0;
        }
        return channel.socket().getLocalPort();
    }

    private synchronized void pergeExpired() {
        Iterator<Entry<UUID, LocalNode>> iterator = knownNodes.entrySet()
                .iterator();

        while (iterator.hasNext()) {
            if (iterator.next().getValue().isExpired()) {
                iterator.remove();
            }
        }
    }

    public void handleConnection(DirectSocket socket) {
        logger.error("TCP connection to UDP discovery service");
    }

    public synchronized NodeInfo[] getNodesList() {
        ArrayList<NodeInfo> result = new ArrayList<NodeInfo>();

        for (LocalNode node : knownNodes.values()) {
            result.add(node.getNode());
        }
        return result.toArray(new NodeInfo[0]);
    }

    public synchronized Map<String, String> getStats() {
        Map<String, String> result = new HashMap<String, String>();

        return result;
    }

    public void run() {
        boolean first = true;

        long deadline;
        ByteBuffer buffer = ByteBuffer
                .allocateDirect(SizeOf.UDP_MAX_PAYLOAD_LENGTH);

        while (true) {
            pergeExpired();
            try {
                // send broadcast message
                buffer.clear();
                ObjectOutputStream out = new ObjectOutputStream(
                        new ByteBufferOutputStream(buffer));

                if (first) {
                    out.writeInt(NODE_ANNOUNCE_REQUEST);
                    first = false;
                } else {
                    out.writeInt(NODE_ANNOUNCE);
                }

                out.writeObject(node.getInfo());
                out.close();
                buffer.flip();

                logger.debug("sending node announce (request) of "
                        + buffer.remaining() + " bytes");

                // this maight fail, but we don't care
                channel.send(buffer, sendAddress);

                long time = System.currentTimeMillis();
                deadline = time + TIMEOUT;

                while (time < deadline) {
                    selector.selectedKeys().clear();

                    // wait for a message to arrive
                    selector.select(deadline - time);

                    buffer.clear();
                    SocketAddress sender = channel.receive(buffer);
                    if (sender != null) {

                        buffer.flip();

                        ObjectInputStream in = new ObjectInputStream(
                                new ByteBufferInputStream(buffer));

                        int opcode = in.readInt();

                        NodeInfo peer = (NodeInfo) in.readObject();
                        in.close();

                        if (!peer.getID().equals(node.getID())) {

                            if (opcode == NODE_ANNOUNCE_REQUEST) {
                                logger
                                        .debug("got node announce request, sending reply");
                                // send node announce to requestor
                                buffer.clear();
                                out = new ObjectOutputStream(
                                        new ByteBufferOutputStream(buffer));
                                out.writeInt(NODE_ANNOUNCE_REPLY);
                                out.writeObject(node.getInfo());
                                out.close();
                                buffer.flip();

                                // this maight fail, but we don't care
                                channel.send(buffer, sender);

                            } else if (opcode == NODE_ANNOUNCE) {
                                logger.debug("got node announce");
                            } else if (opcode == NODE_ANNOUNCE_REPLY) {
                                logger.debug("got node announce reply");
                            } else {
                                logger.warn("received unknown opcode: "
                                        + opcode, new Exception());
                            }
                            synchronized (this) {
                                knownNodes.put(peer.getID(),
                                        new LocalNode(peer));
                            }
                        }
                    }
                    time = System.currentTimeMillis();
                }
            } catch (ClosedChannelException e) {
                logger.error("UdpHandler: caught exception "
                        + "while recieving message ", e);
                return;
            } catch (IOException e) {
                logger.error("UdpHandler: caught exception while "
                        + "recieving message ", e);
                return;

            } catch (ClassNotFoundException e) {
                logger.error("error on receiving broadcast", e);
            }
        }
    }

}