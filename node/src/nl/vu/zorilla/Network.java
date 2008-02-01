package nl.vu.zorilla;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.log4j.Logger;

import nl.vu.zorilla.bamboo.BambooNetwork;
import nl.vu.zorilla.bigNet.BigNet;
import nl.vu.zorilla.stats.Stats;

public abstract class Network {
    
    private static final Logger logger = Logger.getLogger(Network.class);
    
    public static Network createNetwork(Node node) throws Exception {
        if (node.config().useBamboo()) {
            return new BambooNetwork(node);
        } else {
            return new BigNet(node);
        }
    }
  
    public abstract void end(long deadline);

    public abstract void advertise(JobAdvert advert) throws IOException, ZorillaException;
    
    public abstract void killNetwork() throws ZorillaException;
    
    public abstract String getNodeName();
    
    public abstract InetAddress guessInetAddress();
    
    public abstract void getStats(Stats stats);

}