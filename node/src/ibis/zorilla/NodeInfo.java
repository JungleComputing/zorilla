package ibis.zorilla;

import java.io.Serializable;
import java.util.UUID;

import org.apache.log4j.Logger;

import ibis.smartsockets.direct.DirectSocketAddress;
import ibis.zorilla.cluster.Coordinates;


public class NodeInfo implements Serializable {
    
    private static final Logger logger = Logger.getLogger(NodeInfo.class);

    private static final long serialVersionUID = 1L;

    private final UUID id;

    private final String name;
    
    private final String clusterName;

    private final Coordinates coordinates;

    private final DirectSocketAddress address;
    
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

    NodeInfo(UUID id, String name, String clusterName, Coordinates coordinate,
    		DirectSocketAddress address) {
        this.id = id;
        this.name = name;
        this.clusterName = clusterName;
        this.coordinates = coordinate;
        
        this.address = address;

        creationTime = System.currentTimeMillis();
    }

    public UUID getID() {
        return id;
    }
    
    public DirectSocketAddress getAddress() {
        return address;
    }
    
    public Coordinates getCoordinates() {
        return coordinates;
    }

    public String toString() {
        return name;
    }
    
    public String getName() {
        return name;
    }

    public String getClusterName() {
        return clusterName;
    }

    public boolean sameNodeAs(NodeInfo other) {
       return other.id.equals(id);
    }
    
    public boolean newer(NodeInfo other) {
        if (!sameNodeAs(other)) {
            logger.warn("can only compare NodeInfo's created at the same node");
        }
        
        if (other == null) {
            return true;
        }
        return this.creationTime > other.creationTime;
    }
}
