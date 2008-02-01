package nl.vu.zorilla.bigNet;

import java.net.InetSocketAddress;

import nl.vu.zorilla.Node;
import nl.vu.zorilla.ZorillaException;
import nl.vu.zorilla.bigNet.bamboo.BambooNetwork;

public abstract class Network {
    
    public static Network createNetwork(Node node) throws Exception {
        if (!node.config().useBamboo()) {
            throw new ZorillaException("selected non-bamboo network, which is" +
                    " not implemented yet");
        }
            
        return new BambooNetwork(node);
       
    }
    
    public abstract Message getMessage();

    public abstract void flood(Message message, Metric metric, long metricValue
        ) throws NetworkException, ZorillaException;

    public abstract void end(long deadline);

    public abstract InetSocketAddress address();

    
}