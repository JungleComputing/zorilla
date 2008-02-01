package nl.vu.zorilla.bigNet;

import ibis.util.ThreadPool;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;

import org.apache.log4j.Logger;

import smartsockets.direct.DirectSocket;
import smartsockets.direct.SocketAddressSet;

class DiscoveryService implements Service, Runnable {

    public static final int CONNECT_TIMEOUT = 10 * 1000;
    
    public static final int MAX_TRIES = 5;

    private static final Logger logger = Logger
            .getLogger(DiscoveryService.class);

    private SocketAddressSet[] peers;

    private BigNet network;

    DiscoveryService(BigNet network, String[] peers) throws IOException {
        this.network = network;

        this.peers = new SocketAddressSet[peers.length];
        for (int i = 0; i < peers.length; i++) {
            this.peers[i] = SocketAddressSet.getByAddress(peers[i]);
        }
    }

    public void start() {
        ThreadPool.createNew(this, "discovery");
    }

    private NodeInfo doRequest(SocketAddressSet address) throws IOException {
        logger.debug("sending lookup request to " + address);

        DirectSocket socket = network.connect(address,
                BigNet.DISCOVERY_SERVICE, CONNECT_TIMEOUT);

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

    public void handleConnection(DirectSocket socket) {
        logger.debug("handling connection from "
                + socket.getRemoteAddress());

        try {
            // read "request" byte
            socket.getInputStream().read();

            ObjectOutputStream out = new ObjectOutputStream(socket
                    .getOutputStream());
            out.writeObject(network.getNodeInfo());
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
    
    private void doRequests(SocketAddressSet peer) {
        for (int i = 0 ; i < MAX_TRIES; i++) {
            try {
                NodeInfo info = doRequest(peer);
                network.newNode(info);
                return;
            } catch (Exception e) {
                logger.error("lookup of peer: " + peer + " failed", e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //IGNORE
            }
        }
        System.err.println("INITIALIZATION FAILED!!!");
    }
            
    public void run() {
        for (SocketAddressSet peer : peers) {
               doRequests(peer);
        }
    }

    public void handleMessage(DatagramPacket packet) {
        logger.error("received message in tcp based discovery service");
    }
}
