package nl.vu.zorilla.bigNet.gossip;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import nl.vu.zorilla.bigNet.NodeInfo;

class GossipMessage implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private final NodeInfo sender;
    private final NodeInfo receiver;
    private final List<GossipCacheEntry> entries;
    private final boolean replyRequested;
    private final String algorithmName;

    public GossipMessage(NodeInfo sender, NodeInfo receiver, List<GossipCacheEntry> entries, boolean twoWay, String algorithmName) {
        this.sender = sender;
        this.receiver = receiver;
        this.entries = Collections.unmodifiableList(new ArrayList<GossipCacheEntry>(entries));
        this.replyRequested = twoWay;
        this.algorithmName = algorithmName;
    }

    public NodeInfo getSender() {
        return sender;
    }

    public NodeInfo getReceiver() {
        return receiver;
    }

    public List<GossipCacheEntry> getEntries() {
        return entries;
    }

    public boolean replyRequested() {
        return replyRequested;
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

}
