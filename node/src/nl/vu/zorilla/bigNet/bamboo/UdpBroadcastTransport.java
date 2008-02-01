package nl.vu.zorilla.bigNet.bamboo;

import ibis.util.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import org.apache.log4j.Logger;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.bigNet.Function;

/**
 * Listens to incoming UDP broadcast messages.
 */
final class UdpBroadcastTransport implements Runnable {

    public static final int TIMEOUT = 60 * 1000; // 1 minute

    private static final Logger logger = Logger
        .getLogger(UdpBroadcastTransport.class);

    private final BambooNetwork network;
    
    private final Node node;

    private final DatagramChannel channel;

    private final InetSocketAddress sendAddress;

    private final Selector selector;

    private boolean closing = false;

    /**
     * creates a UDP handler for handling broadcast UDP traffic
     */
    public UdpBroadcastTransport(BambooNetwork network, int port, Node node) throws IOException {
        this.network = network;
        this.node = node;

        channel = DatagramChannel.open();

        sendAddress = new InetSocketAddress(InetAddress
            .getByName("255.255.255.255"), port);

        channel.socket().bind(new InetSocketAddress(port));
        channel.socket().setBroadcast(true);

        selector = Selector.open();

        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_READ);

        ThreadPool.createNew(this, "udp broadcast handler");

    }

    /**
     * repeatedly fetch a new message from the network and deliver it to the
     * main "node" class.
     */
    public void run() {
        BambooMessage m = BambooMessage.getMessage();

        long deadline;

        while (!closing) {
            try {

                logger.debug("broadcasting node announce message");
                m.clearMessage();
                m.setSource(network.nodeAddress());
                m.setDestination(Address.UNKNOWN);
                m.setFunction(Function.NODE_DISCOVERY);
                m.setType(MessageType.NODE_ANNOUNCE);
                m.setJobID(null);
                m.setID(Node.generateUUID());
                m.flip();

                m.sendMessage(channel, sendAddress);

                long time = System.currentTimeMillis();
                deadline = time + TIMEOUT;

                while (time < deadline) {
                    selector.selectedKeys().clear();
                    int selected = selector.select(deadline - time);
                    if (selected > 0) {
                        logger.debug("select returned we have a message");
                    }

                    if (m.receiveMessage(channel)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("message received: " + m);
                        }

                        Address source = m.getSource();

                        if (!source.checkNetworkName(node.config().getNetworkName())) {
                            logger.warn("received broadcast message "
                                + "from another Zorilla network, ignoring");
                        } else if (!source.checkVersion(node.getVersion())) {
                            logger.warn("received broadcast message from"
                                + " another Zorilla version, ignoring");
                        } else {

                            if (m.getType() == MessageType.NODE_ANNOUNCE) {
                                network.newLocalNode(source);
                            } else if (m.getType() == MessageType.NODE_ANNOUNCE_REQUEST) {
                                network.newLocalNode(source);
                                // trigger a node annouce message now
                                deadline = 0;
                            } else {
                                logger
                                    .error("unknown message type received in udp"
                                        + " broadcast module");
                            }
                        }
                    }
                    time = System.currentTimeMillis();
                }

            } catch (ClosedChannelException e) {
                synchronized (this) {
                    if (!closing) {
                        logger.error("UdpHandler: caught exception "
                            + "while recieving message ", e);
                    }
                }
                return;
            } catch (IOException e) {
                logger.error("UdpHandler: caught exception while "
                    + "recieving message ", e);
                return;
            }
        }
    }

    public void kill() {
        synchronized (this) {
            closing = true;
        }
        try {
            channel.close();
        } catch (IOException e) {
            // IGNORE
        }
    }
}