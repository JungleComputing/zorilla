package nl.vu.zorilla.bigNet;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;


import org.apache.log4j.Logger;

import smartsockets.direct.DirectSocket;

/**
 * implements ping over udp
 */
class UDPPingService implements PingService {

    public static final int PING_TIMEOUT = 10 * 1000;

    private final Logger logger = Logger.getLogger(UDPPingService.class);

    private BigNet network;

    public UDPPingService(BigNet network) throws IOException {
        this.network = network;
    }

    public double ping(NodeInfo peer) throws IOException {
        double result = ping(peer.getUdpAddress());
        logger.info("ping " + peer + " took " + result);
        return result;
    }
    
    public void start() {
        //NOTHING
    }

    private double ping(SocketAddress peer) throws IOException {
        byte[] data = new byte[Coordinates.SIZE];

        DatagramSocket socket = new DatagramSocket();
        socket.setSoTimeout(PING_TIMEOUT);

        logger.debug("sending ping to " + peer);

        
        data[0] = BigNet.PING_SERVICE;
        // send packet
        DatagramPacket packet = new DatagramPacket(data, 1, peer);

        long start = System.nanoTime();
        socket.send(packet);

        packet.setLength(data.length);
        socket.receive(packet);
        long end = System.nanoTime();
        socket.close();

        long time = end - start;

        double rtt = (double) time / 1000000.0;

        // get coordinates of remote site from packet
        Coordinates remoteCoordinates = new Coordinates(data);
        network.updateCoordinates(remoteCoordinates, rtt);

        logger.debug("ping took " + time + " nanoseconds");
        return rtt;
    }

    public void handleMessage(DatagramPacket packet) {
        try {
            network.send(network.getCoordinates().toBytes(), packet
                    .getSocketAddress());
            logger.debug("ping handled from: " + packet.getSocketAddress());
        } catch (IOException e) {
            logger.error("error on handling ping", e);
        }

    }

    public void handleConnection(DirectSocket socket) {
        logger.error("incoming connection to ping service");
    }

}
