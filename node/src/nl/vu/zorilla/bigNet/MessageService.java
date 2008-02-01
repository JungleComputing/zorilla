package nl.vu.zorilla.bigNet;

import ibis.util.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.log4j.Logger;

import smartsockets.direct.DirectSocket;

import nl.vu.zorilla.JobAdvert;
import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaException;

final class MessageService implements Service {

    private static final Logger logger = Logger.getLogger(MessageService.class);

    public static final int KILL_NETWORK_RADIUS = 100;

    public static final int OPCODE_NETWORK_KILL = 1;

    public static final int OPCODE_JOB_ADVERT_HOPS = 8;

    public static final int OPCODE_JOB_ADVERT_LATENCY = 9;

    private final BigNet network;

    private final Node node;

    MessageService(BigNet network, Node node) {
        this.network = network;
        this.node = node;
    }
    
    public void start() {
        //NOTHING
    }       

    private void killNetwork(int radius) throws ZorillaException {
        if (radius <= 0) {
            logger.debug("not sending out network kill, radius reached 0");
            return;
        }

        try {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            byteOut.write(BigNet.MESSAGE_SERVICE);
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeInt(OPCODE_NETWORK_KILL);
            out.writeInt(radius); // radius of advert
            out.close();

            byte[] data = byteOut.toByteArray();

            NodeInfo[] neighbours = network.getNeighbours();

            for (NodeInfo neighbour : neighbours) {
                network.send(data, neighbour);
            }

        } catch (IOException e) {
            throw new ZorillaException("could not send out network kill", e);
        }
    }

    private void handleNetworkKill(ObjectInputStream in, DatagramPacket packet) {
        try {
            int radius = in.readInt();

            killNetwork(radius - 1);

            node.handleNetworkKill();

        } catch (Exception e) {
            logger.error("could not handle/forward job advert", e);
        }
    }

    public void advertise(JobAdvert advert) throws ZorillaException {

        if (advert.getMetric() != null && advert.getMetric().equals("hops")) {

            // default radius is incremental, start at "1" so we only reach
            // the neighbours
            advertiseHops(advert, advert.getCount() + 1);
        } else if (advert.getMetric() == null
                || advert.getMetric().equals("latency")) {

            double initialRadius = network.distanceToClosestNeighbour();
            if (initialRadius == Double.POSITIVE_INFINITY) {
                initialRadius = 1.0;
            }

            double radius = initialRadius * Math.pow(2, advert.getCount());

            advertiseLatency(advert, radius);
        }
    }

    private void advertiseHops(JobAdvert advert, int ttl)
            throws ZorillaException {
        if (ttl <= 0) {
            logger.debug("not sending out advert, ttl reached 0");
            return;
        }

        try {

            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            byteOut.write(BigNet.MESSAGE_SERVICE);
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeInt(OPCODE_JOB_ADVERT_HOPS);
            out.writeObject(advert);
            out.writeInt(ttl);
            out.close();

            byte[] data = byteOut.toByteArray();

            NodeInfo[] neighbours = network.getNeighbours();

            for (NodeInfo neighbour : neighbours) {
                network.send(data, neighbour);
            }

        } catch (IOException e) {
            throw new ZorillaException("could not send out advert", e);
        }
    }

    private void handleJobAdvertHops(ObjectInputStream in, DatagramPacket packet) {
        try {
            JobAdvert advert = (JobAdvert) in.readObject();
            int ttl = in.readInt();

            advertiseHops(advert, ttl - 1);

            node.handleJobAdvert(advert);

        } catch (Exception e) {
            logger.error("could not handle/forward job advert", e);
        }
    }

    private void advertiseLatency(JobAdvert advert, double distance)
            throws ZorillaException {
        Coordinates here = network.getCoordinates();

        HashSet<NodeInfo> nodes = new HashSet<NodeInfo>();
        nodes.addAll(Arrays.asList(network.getNeighbours()));
        nodes.addAll(Arrays.asList(network.getGossipCache()));

        for (NodeInfo node : nodes) {
            double distanceToNode = node.getCoordinate().distance(here);

            // endless-loop-preventer.
            if (distanceToNode <= 0.0) {
                distanceToNode = 1.0;
            }

            if (distanceToNode < distance) {
                logger.debug("sending advert to " + node + ": "
                        + distanceToNode + " < " + distance);
                try {
                    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                    byteOut.write(BigNet.MESSAGE_SERVICE);
                    ObjectOutputStream out = new ObjectOutputStream(byteOut);
                    out.writeInt(OPCODE_JOB_ADVERT_HOPS);
                    out.writeObject(advert);
                    out.writeDouble(distance - distanceToNode);
                    out.close();

                    byte[] data = byteOut.toByteArray();

                    network.send(data, node);
                } catch (IOException e) {
                    throw new ZorillaException("could not send out advert", e);
                }
            } else {
                logger.debug("not sending advert to " + node
                        + " too far away : " + distanceToNode + " > "
                        + distance);
            }

        }
    }

    private void handleJobAdvertLatency(ObjectInputStream in,
            DatagramPacket packet) {
        try {
            JobAdvert advert = (JobAdvert) in.readObject();
            double radius = in.readDouble();

            advertiseLatency(advert, radius);

            node.handleJobAdvert(advert);

        } catch (Exception e) {
            logger.error("could not handle/forward job advert", e);
        }
    }

    public void killNetwork() throws ZorillaException {
        killNetwork(KILL_NETWORK_RADIUS);
    }

    public void handleMessage(DatagramPacket packet) {
        try {
            InputStream bytes = new ByteArrayInputStream(packet.getData());
            bytes.skip(1);
            ObjectInputStream in = new ObjectInputStream(bytes);
            int opcode = in.readInt();

            switch (opcode) {
            case OPCODE_NETWORK_KILL:
                handleNetworkKill(in, packet);
                break;
            case OPCODE_JOB_ADVERT_HOPS:
                handleJobAdvertHops(in, packet);
                break;
            case OPCODE_JOB_ADVERT_LATENCY:
                handleJobAdvertLatency(in, packet);
                break;
            default:
                logger.error("unknown opcode in received message: " + opcode);
            }
        } catch (IOException e) {
            logger.error("exception on handling message", e);
        }
    }

    public void handleConnection(DirectSocket socket) {
        logger.error("connection to UDP based message service");
    }

}
