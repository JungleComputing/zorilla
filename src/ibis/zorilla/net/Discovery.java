package ibis.zorilla.net;

import ibis.zorilla.Config;
import ibis.zorilla.Node;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

public class Discovery extends Thread {

    // how often do we re-do the discovery process to update node info? (5 min)
    public static final int DISCOVERY_INTERVAL = 5 * 60 * 1000;

    private static final Logger logger = Logger
            .getLogger(Discovery.class);

    private final Set<SmartSocketsAddress> addresses;

    private final Network network;

    public Discovery(Node node, Network network) throws IOException {
        this.network = network;

        addresses = new HashSet<SmartSocketsAddress>();

        for (String string : node.config().getStringList(Config.PEERS)) {
            addPeer(string);
        }
        
        setName("dicovery module");
        setDaemon(true);
        start();
    }

    public synchronized void addPeer(String address) {
        SmartSocketsAddress smsAddress = null;
        try {
            smsAddress = new SmartSocketsAddress(address);
        } catch (Exception e) {
            logger.warn("invalid peer address: " + address, e);
        }
        
        if (smsAddress != null) {
            addresses.add(smsAddress);

            // nudge the discovery thread
            notifyAll();
        }
    }

   
   
    public void run() {
        while (true) {
            for (SmartSocketsAddress peer : getAddresses()) {
                //tries to esablish a connection if one is not already present
                //we then do exactly nothing with this connection ;-)
                //will update the NodeInfo as a side-effect
                try {
                    network.getConnection(peer);
                } catch (NetworkException e) {
                    logger.warn("could not get connection to peer", e);
                }
            }

            synchronized (this) {
                try {
                    wait(DISCOVERY_INTERVAL);
                } catch (InterruptedException e) {
                    // IGNORE
                }
            }
        }
    }

    private synchronized SmartSocketsAddress[]  getAddresses() {
        return addresses.toArray(new SmartSocketsAddress[addresses.size()]);
    }

   
}
