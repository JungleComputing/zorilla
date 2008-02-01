package nl.vu.zorilla.bigNet.gossip;

import nl.vu.zorilla.bigNet.NodeInfo;

interface GossipAlgorithm {
    
    /**
     * @return The reply for the request
     */
    GossipMessage handleRequest(GossipMessage request);

    void doGossip(int timeout);

    NodeInfo[] getNodes();

    NodeInfo[] getRandom(int n);

    String getName();

    String getStatus();
    
    void start();
}
