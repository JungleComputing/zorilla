package ibis.zorilla.gossip;

import ibis.zorilla.NodeInfo;

import java.io.IOException;
import java.io.Serializable;

final class GossipCacheEntry implements Serializable {

    private static final long TIMEOUT = 60 * 60 * 1000;

    private static final long serialVersionUID = 1L;

    private NodeInfo info;

    private int age;

    private long expirationTime;

    GossipCacheEntry(NodeInfo info) {
        this.info = info;
        age = 0;
        expirationTime = System.currentTimeMillis() + TIMEOUT;
    }

    public boolean hasExpired() {
        return System.currentTimeMillis() > expirationTime;
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

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(info);
        out.writeInt(age);
        out.writeLong(expirationTime - System.currentTimeMillis());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        info = (NodeInfo) in.readObject();
        age = in.readInt();
        expirationTime = in.readLong() + System.currentTimeMillis();
    }
}