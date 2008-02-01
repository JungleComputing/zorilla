package nl.vu.zorilla.bigNet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;

import org.apache.log4j.Logger;

import smartsockets.direct.DirectSocket;

/**
 * implements ping over udp
 */
class TCPPingService implements PingService {

    public static final int PING_TIMEOUT = 10 * 1000;

    public static final int TRIES = 4;

    private final Logger logger = Logger.getLogger(TCPPingService.class);

    private BigNet network;

    public TCPPingService(BigNet network) throws IOException {
        this.network = network;
    }

    public double ping(NodeInfo peer) throws IOException {
        double result = Double.MAX_VALUE;

        DirectSocket socket = network.connect(peer, BigNet.PING_SERVICE,
                PING_TIMEOUT);
        socket.setTcpNoDelay(true);

        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();

        // get coordinates from peer
        byte[] coordinateBytes = new byte[Coordinates.SIZE];
        int remaining = Coordinates.SIZE;
        int offset = 0;
        while (remaining > 0) {
            int read = in.read(coordinateBytes, offset, remaining);
            if (read == -1) {
                throw new IOException("could not read Coordinates");
            }
            offset += read;
            remaining -= read;
        }
        Coordinates remoteCoordinates = new Coordinates(coordinateBytes);

        for (int i = 0; i < TRIES; i++) {
            long start = System.nanoTime();
            out.write(i);
            int reply = in.read();
            long end = System.nanoTime();
            if (reply != i) {
                throw new IOException("ping failed, wrong reply: " + reply);
            }

            long time = end - start;
            logger.debug("ping took " + time + " nanoseconds");
            double rtt = (double) time / 1000000.0;
            logger.info("ping " + peer + " took " + rtt);

            if (rtt < result) {
                result = rtt;
            }
        }
        socket.close();

        // update coordinates
        network.updateCoordinates(remoteCoordinates, result);

        logger.info("ping " + peer + " final result: " + result);

        return result;
    }

    public void handleConnection(DirectSocket socket) {
        try {
            socket.setTcpNoDelay(true);

            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            // send coordinates
            out.write(network.getCoordinates().toBytes());

            for (int i = 0; i < TRIES; i++) {
                int read = in.read();
                out.write(read);
            }
            socket.close();
        } catch (IOException e) {
            logger.error("error on handling ping", e);
        }
    }

    public void start() {
        // NOTHING
    }

    public void handleMessage(DatagramPacket packet) {
        logger.error("received message in TCP ping service");
    }

}
