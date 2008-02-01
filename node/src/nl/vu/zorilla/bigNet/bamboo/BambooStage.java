package nl.vu.zorilla.bigNet.bamboo;

//import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.UUID;

import org.apache.log4j.Logger;

import nl.vu.zorilla.ZorillaError;
import nl.vu.zorilla.ZorillaException;

import nl.vu.zorilla.util.Enums;

import ostore.network.NetworkMessage;
import ostore.util.InputBuffer;
import ostore.util.NodeId;
import ostore.util.OutputBuffer;
import ostore.util.QSException;
import ostore.util.QuickSerializable;
import seda.sandStorm.api.ConfigDataIF;
import seda.sandStorm.api.QueueElementIF;
import seda.sandStorm.api.StagesInitializedSignal;
import bamboo.api.BambooDownNodeAdd;
import bamboo.api.BambooFloodInit;
import bamboo.api.BambooNeighborReq;
import bamboo.api.BambooNeighbors;
import bamboo.api.BambooRouteDeliver;
import bamboo.api.BambooRouterAppRegReq;
import bamboo.api.BambooRouterAppRegResp;
import bamboo.util.StandardStage;
import bamboo.vivaldi.VirtualCoordinate;
import bamboo.vivaldi.VivaldiReplyVC;
import bamboo.vivaldi.VivaldiRequestVC;

public class BambooStage extends StandardStage {

    private static final int RECENT_MESSAGE_CACHE_SIZE = 100;

    private static final Logger logger = Logger.getLogger(BambooStage.class);

    private static final long applicationID = bamboo.router.Router
        .applicationID(BambooStage.class);

    private final BambooTransport bambooTransport;

    private boolean initialized = false;

    private LinkedList waitQ;

    private LinkedHashSet recentFloodMessages;

    // shared with bamboo transport
    BigInteger bambooID = null;

    VirtualCoordinate coordinate = null;

    NodeId[] neighbors = new NodeId[0];

    public static class Payload implements QuickSerializable {
        private byte[] message;

        private boolean flood;

   

        public Payload(BambooMessage message, boolean flood) {
            this.message = message.toBytes();
            this.flood = flood;
   
        }

        public Payload(InputBuffer in) {
            int messageSize = in.nextInt();

            message = new byte[messageSize];
            in.nextBytes(message, 0, messageSize);

            flood = in.nextBoolean();

       
        }

        BambooMessage message() throws IOException {
            return BambooMessage.fromBytes(message);
        }

        boolean flood() {
            return flood;
        }

        public void serialize(OutputBuffer out) {
            out.add(message.length);
            out.add(message);
            out.add(flood);
         
        }

      
    }

    public static class ZorillaMessageMessage extends NetworkMessage {
        private byte[] message;

        public ZorillaMessageMessage(NodeId destination, BambooMessage message) {
            super(destination, false);
            this.message = message.toBytes();
        }

        BambooMessage message() throws IOException {
            return BambooMessage.fromBytes(message);
        }

        public ZorillaMessageMessage(InputBuffer buffer) throws QSException {
            super(buffer);

            int messageSize = buffer.nextInt();

            message = new byte[messageSize];
            buffer.nextBytes(message, 0, messageSize);

        }

        public void serialize(OutputBuffer buffer) {
            super.serialize(buffer);
            buffer.add(message.length);
            buffer.add(message);
        }

        public Object clone() throws CloneNotSupportedException {
            ZorillaMessageMessage result = (ZorillaMessageMessage) super
                .clone();
            result.message = message;
            return result;
        }
    }

    public static class SendMessageEvent implements QueueElementIF {
        BambooMessage message;

        SendMessageEvent(BambooMessage message) {
            this.message = message;
        }
    }

    public static class NewLocalNodeEvent implements QueueElementIF {
        Address address;

        NewLocalNodeEvent(Address address) {
            this.address = address;
        }
    }

    public static class SendFloodMessageEvent implements QueueElementIF {
        BambooMessage message;



        SendFloodMessageEvent(BambooMessage message) {
            this.message = message;

        }
    }

    public synchronized Location getLocation() {
        if (coordinate == null) {
            return new Location(bambooID);
        } else {
            return new Location(bambooID, coordinate.getCoordinates());
        }
    }

    public synchronized NodeId[] getNeighbors() {
        return neighbors.clone();
    }

    // called by BambooModule to send a message
    void sendMessage(BambooMessage m) {
        classifier.dispatch_later(new SendMessageEvent(m), 0);
    }

    void newLocalNode(Address address) {
        classifier.dispatch_later(new NewLocalNodeEvent(address), 0);
    }

    // called by BambooModule to flood a message
    void floodMessage(BambooMessage m) {
        classifier.dispatch_later(new SendFloodMessageEvent(m), 0);
    }

    public BambooStage() {
        super();

        try {
            ostore.util.TypeTable.register_type(Payload.class);
        } catch (Exception e) {
            throw new ZorillaError("exception on registerring type", e);
        }

        event_types = new Class[] { StagesInitializedSignal.class,
            BambooRouteDeliver.class, BambooRouterAppRegResp.class,
            SendMessageEvent.class, SendFloodMessageEvent.class,
            BambooNeighbors.class, NewLocalNodeEvent.class,
            VivaldiReplyVC.class };

        inb_msg_types = new Class[] { ZorillaMessageMessage.class };

        bambooTransport = BambooTransport.getInstance();
        bambooTransport.setStage(this);

        waitQ = new LinkedList();

        recentFloodMessages = new LinkedHashSet();

    }

    public void init(ConfigDataIF config) throws Exception {
        super.init(config);

        classifier.dispatch_later(new VivaldiRequestVC(my_sink, null), 5000);
        classifier.dispatch_later(new BambooNeighborReq(my_sink), 5000);

    }

    private void handleFloodMessage(BambooMessage message) {
        UUID messageID = message.getId();
        if (recentFloodMessages.remove(messageID)) {
            // this message has already been received recently
            // remove-and-then-add to put it at the back of the
            // recent message list
            recentFloodMessages.add(messageID);
            logger.debug("ignoring duplicate flood message: " + messageID);
            return;
        } else {
            recentFloodMessages.add(messageID);
            if (recentFloodMessages.size() > RECENT_MESSAGE_CACHE_SIZE) {
                recentFloodMessages.remove(recentFloodMessages.iterator()
                    .next());
            }
            int ttl = message.getTtl();

            logger.debug("received flood message: " + messageID + " with ttl "
                + ttl);
            bambooTransport.messageReceived(message);

            ttl--;

            if (ttl <= 0) {
                // prune flood tree
                logger.debug("not forwarding due to ttl");
                return;
            }

            message.setTtl(ttl);

            BambooFloodInit init = new BambooFloodInit(applicationID,
                new Payload(message, true), ttl);
            dispatch(init);
        }
    }

    public void handleEvent(QueueElementIF item) {

        logger.debug("handling event");

        if (!initialized) {
            if (item instanceof StagesInitializedSignal) {
                dispatch(new BambooRouterAppRegReq(applicationID, false, false,
                    false, my_sink));

            } else if (item instanceof BambooRouterAppRegResp) {
                BambooRouterAppRegResp response = (BambooRouterAppRegResp) item;

                if (response.success != true) {
                    throw new ZorillaError("error on initializing bamboo stage");
                }

                initialized = true;
                logger.debug("bamboo stage initialized");
                setID(response.node_guid);
                while (!waitQ.isEmpty()) {
                    handleEvent((QueueElementIF) waitQ.removeFirst());
                }
            } else {
                // save this event for when we are initialized
                waitQ.addLast(item);
            }
        } else if (item instanceof VivaldiReplyVC) {
            logger.debug("handling vivaldiReplyVC event");

            VivaldiReplyVC reply = (VivaldiReplyVC) item;

            synchronized (this) {
                coordinate = reply.coordinate;
            }

            classifier
                .dispatch_later(new VivaldiRequestVC(my_sink, null), 5000);
        } else if (item instanceof BambooNeighbors) {
            logger.debug("handling bambooneighbors event");

            BambooNeighbors reply = (BambooNeighbors) item;

            logger.debug("bamboo neighbours now: " + reply.toString());

            synchronized (this) {
                neighbors = reply.neighbors.clone();
            }

            classifier.dispatch_later(new BambooNeighborReq(my_sink), 5000);

        } else if (item instanceof SendMessageEvent) {
            // logger.debug("handling sendmessage event");
            //
            // SendMessageEvent event = (SendMessageEvent) item;
            //
            // logger.debug("sending: " + event.message.getId());
            //
            // // normal send
            // BigInteger destination =
            // event.message.getDestination().location()
            // .bambooID();
            //
            // // network metric ignored...
            // BambooRouteInit init = new BambooRouteInit(destination,
            // applicationID, false, false, new Payload(event.message, false,
            // Network.Metric.REPLICAS));
            // dispatch(init);

            logger.debug("handling sendmessage event");

            SendMessageEvent event = (SendMessageEvent) item;

            logger.debug("sending: " + event.message.getId());

            NodeId dest = new NodeId(event.message.getDestination().port(),
                event.message.getDestination().inetAddress());

            ZorillaMessageMessage m = new ZorillaMessageMessage(dest,
                event.message);
            m.timeout_sec = 300;

            dispatch(m);

        } else if (item instanceof SendFloodMessageEvent) {
            logger.debug("handling sendfloodmessage event");

            SendFloodMessageEvent event = (SendFloodMessageEvent) item;

            logger.debug("sending flood message: " + event.message.getId());

            BambooFloodInit init;

            init = new BambooFloodInit(applicationID,
                new Payload(event.message, true), event.message
                    .getTtl());
            dispatch(init);
        } else if (item instanceof NewLocalNodeEvent) {
            logger.debug("handling newlocalnode event");

            NewLocalNodeEvent event = (NewLocalNodeEvent) item;

            NodeId nodeId = new NodeId(event.address.port(), event.address
                .inetAddress());
            BambooDownNodeAdd add = new BambooDownNodeAdd(nodeId);

            logger.debug("dispatching BambooDownNodeAdd");

            dispatch(add);

        } else if (item instanceof BambooRouteDeliver) {
            logger.debug("handling bamboo route deliver event");

            BambooRouteDeliver event = (BambooRouteDeliver) item;
            Payload payload = (Payload) event.payload;
            try {
                BambooMessage message = payload.message();

                logger
                    .debug("bamboo route deliver message: " + message.getId());

                if (payload.flood()) {
                    handleFloodMessage(message);
                } else {
                    bambooTransport.messageReceived(message);
                }
            } catch (IOException e) {
                logger.warn(
                    "could not get zorilla message from bamboo message", e);
            }
        } else if (item instanceof ZorillaMessageMessage) {

            logger.debug("got ZorillaMessageMessage");

            ZorillaMessageMessage event = (ZorillaMessageMessage) item;

            logger.debug("message from " + event.peer);

            try {

                bambooTransport.messageReceived(event.message());
            } catch (IOException e) {
                logger.warn(
                    "could not get zorilla message from bamboo message", e);
            }
        }

    }

    public synchronized void setID(BigInteger id) {
        bambooID = id;
        notifyAll();
    }

    public synchronized BigInteger getID() {
        while (bambooID == null) {
            try {
                logger
                    .debug("waiting for the bamboo thread to set the bamboo id");
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return bambooID;
    }
}
