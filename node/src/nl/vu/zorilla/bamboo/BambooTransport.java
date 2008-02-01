package nl.vu.zorilla.bamboo;

import ibis.util.ThreadPool;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import ostore.util.NodeId;

import bamboo.lss.ASyncCoreImpl;
import bamboo.lss.DustDevil;

import seda.sandStorm.api.StageNameAlreadyBoundException;
import seda.sandStorm.main.SandstormConfig;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaError;
import nl.vu.zorilla.ZorillaException;

public final class BambooTransport implements Runnable {

    private static Logger logger = Logger.getLogger(BambooTransport.class);

    private static BambooTransport instance = null;

    static synchronized BambooTransport getInstance() {
        return instance;
    }

    private static synchronized void setInstance(BambooTransport bambooModule) {
        instance = bambooModule;
    }

    private BambooStage stage;

    private final BigInteger bambooID;

    private final Node node;
    private final BambooNetwork network;

    private final InetSocketAddress address;

    private final InetSocketAddress[] gateways;

    public BambooTransport(Node node, BambooNetwork network, InetSocketAddress address)
        throws ZorillaException {
        this.node = node;
        this.network = network;
        this.address = address;

        //FIXME: broken
        InetSocketAddress[] peers = null ;
        
        if (peers.length == 0) {
            gateways = new InetSocketAddress[]{ address };
        } else {
            gateways = peers.clone();
        }

        setInstance(this);

        ThreadPool.createNew(this, "bamboo");

        // bambooID = null;
        waitForStage();

        synchronized (this) {
            bambooID = stage.getID();

            if (bambooID == null) {

                throw new ZorillaException("could not initialize bamboo");
            }
            logger.debug("node initialized as bamboo node: " + bambooID);
        }

    }

    public synchronized void setStage(BambooStage stage) {
        this.stage = stage;

        notifyAll();
    }

    private synchronized void waitForStage() throws ZorillaException {
        while (stage == null) {
            logger.debug("waiting for stage");
            try {
                // bamboo will kill node if needed.
                wait();
            } catch (InterruptedException e) {
                // IGNORE
            }
        }
        if (stage == null) {
            throw new ZorillaException("could not find bamboo stage");
        }
        logger.debug("stage initialized");
        return;
    }

    public Location location() {
        return stage.getLocation();
    }

    public synchronized void send(BambooMessage message) {
        assert message.getSource() != null : "message source not filled in";
        assert message.getDestination() != null : "message destination not filled in";
        assert message.getId() != null : "message id not filled in";
        assert message.getType() != null : "message with no type send";
        assert message.getFunction() != null : "message function not filled in";
        assert message.getJobID() != null : "message jobID not filled in";

        stage.sendMessage(message);
    }

    public synchronized void flood(BambooMessage message) {
       
        stage.floodMessage(message);
        // also deliver to self...
        messageReceived(message);
    }

    // called by the stage to deliver a message to this node
    synchronized void messageReceived(BambooMessage m) {
        Address source = m.getSource();

        if (!source.checkNetworkName(node.config().getNetworkName())) {
            logger
                .warn("message received from different Zorilla network , ignoring");
            return;
        }

        if (!source.checkVersion(node.getVersion())) {
            logger
                .warn("message received from different Zorilla version, ignoring");
            return;
        }

        network.messageReceived(m);
    }

    public synchronized void kill() {
        // NOTHING
    }

    public void run() {
        try {
            String[] nodeID = { "node_id="
                + address.getAddress().getHostAddress() + ":"
                + address.getPort() };
            SandstormConfig sandstormConfig = new SandstormConfig(nodeID);

            // network
            sandstormConfig.addStage("Network", "bamboo.network.Network",
                nodeID);

            // router
            ArrayList<String> routerArgs = new ArrayList<String>();
            routerArgs.add(nodeID[0]);
            routerArgs.add("gateway_count=" + gateways.length);
            for (int i = 0; i < gateways.length; i++) {
                routerArgs.add("gateway_" + i + "=" + gateways[i].getAddress().getHostAddress() +
                   ":" + gateways[i].getPort());
            }
            routerArgs.add("immediate_join=true");
            routerArgs.add("lookup_rt_alarm_period=30");

              sandstormConfig.addStage("Router", "bamboo.router.Router",
                routerArgs.toArray(new String[0]));

            String[] vivaldiArgs = new String[5];
            vivaldiArgs[0] = nodeID[0];
            vivaldiArgs[1] = "eavesdrop_pings=true";
            vivaldiArgs[2] = "generate_pings=true";
            vivaldiArgs[3] = "use_reverse_ping=true";

            if (Location.COORDINATE_DIMENSIONS == 3) {
                vivaldiArgs[4] = "vc_type=3d";
            } else if (Location.COORDINATE_DIMENSIONS == 5) {
                vivaldiArgs[4] = "vc_type=5d";
            } else {
                throw new ZorillaError("unknown number of dimension in virtual"
                    + "coordinate system");
            }

            sandstormConfig.addStage("Vivaldi", "bamboo.vivaldi.Vivaldi",
                vivaldiArgs);

            // zorilla interface
            sandstormConfig.addStage("Zorilla",
                "nl.vu.zorilla.bamboo.BambooStage", nodeID);

            DustDevil.set_acore_instance(new ASyncCoreImpl());
            DustDevil dustDevil = new DustDevil();
            dustDevil.main(sandstormConfig);

        } catch (StageNameAlreadyBoundException e) {
            throw new ZorillaError("could not initialize bamboo properly", e);
        } catch (Exception e) {
            throw new ZorillaError("could not initialize bamboo properly", e);
        }

        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

        // run bamboo main loop
        DustDevil.acore_instance().async_main();
    }

    public void newLocalNode(Address address) {
        stage.newLocalNode(address);
    }

    public Neighbour[] getNeighbours() {
        NodeId[] ids = stage.getNeighbors();

        Neighbour[] result = new Neighbour[ids.length];

        for (int i = 0; i < ids.length; i++) {
            result[i] = new Neighbour(new Address(ids[i].address(), ids[i]
                .port(), node), 0);
        }

        return result;
    }

}
