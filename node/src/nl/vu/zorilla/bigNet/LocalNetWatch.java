package nl.vu.zorilla.bigNet;

import ibis.util.ThreadPool;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.Map.Entry;

import nl.vu.zorilla.io.ByteBufferInputStream;
import nl.vu.zorilla.io.ByteBufferOutputStream;
import nl.vu.zorilla.util.SizeOf;

import org.apache.log4j.Logger;

/**
 * Maintains a list of all nodes reachable through UDP broadcast (the
 * neighbourhood of this node)
 */
final class LocalNetWatch implements Runnable {

    public static final int NODE_ANNOUNCE = 0;

    public static final int NODE_ANNOUNCE_REQUEST = 1;

    public static final int NODE_ANNOUNCE_REPLY = 2;

    // timeout before we send another broadcast
    public static final long TIMEOUT = 5 * 60 * 1000; // 5 minutes

    // time after which an entry expires
    public static final long NEIGHBOUR_LIFETIME = 25 * 60 * 1000; // 25

    // minutes

    private static final Logger logger = Logger.getLogger(LocalNetWatch.class);

    private final DatagramChannel channel;

    private final InetSocketAddress sendAddress;

    private final Selector selector;

    private final BigNet network;

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

    public LocalNetWatch(int port, BigNet network) throws IOException {
        this.network = network;

        knownNodes = new HashMap<UUID, LocalNode>();

        channel = DatagramChannel.open();

        sendAddress = new InetSocketAddress(InetAddress
                .getByName("255.255.255.255"), port);

        channel.socket().bind(new InetSocketAddress(port));
        channel.socket().setBroadcast(true);

        selector = Selector.open();

        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_READ);
    }
    
    void start() {
        ThreadPool.createNew(this, "neighbourhood watch");
    }        

    public int getPort() {
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

                out.writeObject(network.getNodeInfo());
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

                        if (!peer.getID().equals(network.getNodeID())) {

                            if (opcode == NODE_ANNOUNCE_REQUEST) {
                                logger
                                        .debug("got node announce request, sending reply");
                                // send node announce to requestor
                                buffer.clear();
                                out = new ObjectOutputStream(
                                        new ByteBufferOutputStream(buffer));
                                out.writeInt(NODE_ANNOUNCE_REPLY);
                                out.writeObject(network.getNodeInfo());
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
                                if (knownNodes.remove(peer.getID()) == null) {
                                    network.newNode(peer);
                                }

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