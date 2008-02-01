package nl.vu.zorilla.bigNet;

import java.io.Serializable;
import java.net.SocketAddress;
import java.util.UUID;

import smartsockets.direct.SocketAddressSet;

public class NodeInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final UUID id;

    private final String name;

    private final Coordinates coordinate;

    private final SocketAddressSet address;
    private final SocketAddress udpAddress;
    
    private final long creationTime;

    static NodeInfo newest(NodeInfo one, NodeInfo other) {
        if (one == null) {
            return other;
        } else if (other == null) {
            return one;
        } else if (one.creationTime > other.creationTime) {
            return one;
        } else {
            return other;
        }
    }

    NodeInfo(UUID id, String name, Coordinates coordinate,
            SocketAddressSet address, SocketAddress udpAddress) {
        this.id = id;
        this.name = name;
        this.coordinate = coordinate;
        
        this.address = address;
        this.udpAddress = udpAddress;

        creationTime = System.currentTimeMillis();
    }

    public UUID getID() {
        return id;
    }
    
    SocketAddressSet getAddress() {
        return address;
    }
    
    public SocketAddress getUdpAddress() {
        return udpAddress;
    }

    public Coordinates getCoordinate() {
        return coordinate;
    }

    public String toString() {
        return name;
    }

    public boolean sameNodeAs(NodeInfo other) {
       return other.id.equals(id);
    }
}
