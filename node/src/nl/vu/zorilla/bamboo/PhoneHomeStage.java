
package nl.vu.zorilla.bamboo;

import java.io.File;
import java.io.FileNotFoundException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.Random;

import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.StagesInitializedSignal;
import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.util.RandomUtil;
import bamboo.util.StandardStage;
import bamboo.vivaldi.VirtualCoordinate;
import bamboo.vivaldi.VivaldiReplyVC;
import bamboo.vivaldi.VivaldiRequestVC;

public class PhoneHomeStage extends StandardStage {

    private static final long applicationID = bamboo.router.Router
            .applicationID(PhoneHomeStage.class);

    private boolean initialized = false;

    BigInteger bambooID = null;

    private LinkedList waitQ;

    VirtualCoordinate coordinate = null;

    NodeId home;

    private Formatter coordinateFile = null;
    
    private Random random;

    public static class PhoneHomeMessage extends NetworkMessage {
        public VirtualCoordinate coordinate;

        public BigInteger node;

        public PhoneHomeMessage(NodeId destination,
                VirtualCoordinate coordinate, BigInteger node) {
            super(destination, false);
            this.coordinate = coordinate;
            this.node = node;
        }

        public PhoneHomeMessage(InputBuffer buffer) throws QSException {
            super(buffer);
            coordinate = (VirtualCoordinate) buffer.nextObject();
            node = buffer.nextBigInteger();
        }

        public void serialize(OutputBuffer buffer) {
            super.serialize(buffer);
            buffer.add(coordinate);
            buffer.add(node);
        }

        public Object clone() throws CloneNotSupportedException {
            PhoneHomeMessage result = (PhoneHomeMessage) super.clone();
            result.coordinate = this.coordinate;
            result.node = this.node;
            return result;
        }
    }

    public PhoneHomeStage() {
        super();

        event_types = new Class[] { StagesInitializedSignal.class,
                BambooRouterAppRegResp.class, VivaldiReplyVC.class };

        inb_msg_types = new Class[] { PhoneHomeMessage.class };

        waitQ = new LinkedList();
        
        random = new Random();

    }

    private long randomWaitTime() {
        return (long) RandomUtil.random_gaussian(60000, 10000,
            random);
    }


    public void init(ConfigDataIF config) throws Exception {
        super.init(config);

        String homeAddressString = this.config_get_string(config,
                "home_address");

        if (homeAddressString == null) {
            throw new Exception("could not find home_address config var");
        }

        InetAddress homeAddress = InetAddress.getByName(homeAddressString);

        String homePortString = this.config_get_string(config, "home_port");

        if (homePortString == null) {
            throw new Exception("could not find home_port config var");
        }

        int homePort = Integer.parseInt(homePortString);

        home = new NodeId(homePort, homeAddress);
        
        logger.debug("sending phone home messages to: " + home);

        
        classifier.dispatch_later(new VivaldiRequestVC(my_sink, null), 5000);
        
    }

    // called by the stage to deliver a "phone home" message
    private void phoneHomeReceived(BigInteger node, double[] coordinate,
            InetAddress inetAddress, int port) {
        if (coordinateFile == null) {
            try {
                coordinateFile = new Formatter(new File("coordinates"));
            } catch (FileNotFoundException e) {
                logger.error("could not open file to write coordinates to", e);
                return;
            }
        }

        if (coordinate.length == 3) {
            coordinateFile.format("%d %s:%d %d %s %s %s%n", System
                    .currentTimeMillis(), inetAddress.getCanonicalHostName(),
                    port, node, Double.toString(coordinate[0]), Double
                            .toString(coordinate[1]), Double
                            .toString(coordinate[2]));
        } else if (coordinate.length == 5) {
            coordinateFile.format("%d %s:%d %d %s %s %s %s %s%n", System
                    .currentTimeMillis(), inetAddress.getCanonicalHostName(),
                    port, node, Double.toString(coordinate[0]), Double
                            .toString(coordinate[1]), Double
                            .toString(coordinate[2]), Double
                            .toString(coordinate[3]), Double
                            .toString(coordinate[4]));
        } else {
            logger.error("got " + coordinate.length
                    + " dimensional coordinate in phone home message, "
                    + "can only handle 3 and 5");
        }
        coordinateFile.flush();
    }

    public synchronized void handleEvent(QueueElementIF item) {
        if (!initialized) {
            if (item instanceof StagesInitializedSignal) {
                logger.debug("sending app regreq");
                dispatch(new BambooRouterAppRegReq(applicationID, false, false,
                        false, my_sink));
            } else if (item instanceof BambooRouterAppRegResp) {
                BambooRouterAppRegResp response = (BambooRouterAppRegResp) item;

                if (response.success != true) {
                    logger.error("error on initializing phone home stage");
                    throw new Error("error on initializing bamboo stage");
                }

                initialized = true;
                logger.debug("bamboo stage initialized");
                bambooID =  response.node_guid;
                while (!waitQ.isEmpty()) {
                    handleEvent((QueueElementIF) waitQ.removeFirst());
                }
            } else {
                // save this event for when we are initialized
                waitQ.addLast(item);
            }
        } else if (item instanceof VivaldiReplyVC) {
            logger.debug("got vivaldi coordinate");
            
            VivaldiReplyVC reply = (VivaldiReplyVC) item;
            coordinate = reply.coordinate;
            
            PhoneHomeMessage message = new PhoneHomeMessage(home, coordinate,
                    bambooID);
            message.timeout_sec = 300;

            dispatch(message);

            
            classifier
                    .dispatch_later(new VivaldiRequestVC(my_sink, null), randomWaitTime());
        } else if (item instanceof PhoneHomeMessage) {
            logger.debug("got Phone Home Message");

            PhoneHomeMessage event = (PhoneHomeMessage) item;

            logger.debug("phone home from " + event.peer + " @ "
                    + event.coordinate);

            phoneHomeReceived(event.node, event.coordinate.getCoordinates(),
                    event.peer.getAddress(), event.peer.port());
        }
    }
}
