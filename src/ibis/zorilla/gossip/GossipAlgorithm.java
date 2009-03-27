package ibis.zorilla.gossip;

import ibis.zorilla.NodeInfo;

interface GossipAlgorithm {

    /**
     * @return The reply for the request
     */
    GossipMessage handleRequest(GossipMessage request);

    void doGossip(int timeout);

    NodeInfo[] getNodes();

    NodeInfo[] getRandomNodes(int n);
    
    NodeInfo getRandomNode();    

    String getName();

    void start();

    NodeInfo[] getFallbackNodes();

    Stats getStats();

}
