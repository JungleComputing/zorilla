package nl.vu.zorilla.gossip;

import java.io.Serializable;

import nl.vu.zorilla.NodeInfo;

final class GossipCacheEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private NodeInfo info;

    private int age;

    GossipCacheEntry(NodeInfo info) {
        this.info = info;
        age = 0;
    }

    void incAge() {
        age++;
    }

    void setAge(int age) {
        this.age = age;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public int getAge() {
        return age;
    }

    public NodeInfo getInfo() {
        return info;
    }
    
    public String toString() {
        return age + " " + info;
    }
    
    public boolean sameNodeAs(GossipCacheEntry other) {
        return info.sameNodeAs(other.info);
    }
}